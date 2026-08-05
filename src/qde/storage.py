from pathlib import Path

import duckdb
import pandas as pd

from qde.loaders import NoNewData, load_ohlcv


def _bars_path(symbol: str, source: str, interval: str = "1d", base_dir: str = "data") -> Path:
    """Build the bronze path for one OHLCV bar series.

    The new source of truth for where bars live, replacing ``_ohlcv_path``.
    Hive-partitioned by the keys we actually filter on -- source, symbol,
    interval -- with the whole time series kept in a single file.

    Unlike the microstructure lake (see ``qde.stream.paths.bronze_path``),
    bars are NOT partitioned by date: a daily series is one row per day, so a
    ``date=`` partition would mean thousands of one-row files (the small-files
    problem). ``date`` stays a column inside the file instead. Same
    partitioning idea, different grain -- because bars and ticks have
    different shapes (group-by-shape).

    Args:
        symbol (str): a ticker symbol, e.g. "BTCUSDT". A partition key.
        source (str): the source, e.g. "binance". A partition key.
        interval (str, optional): bar size, e.g. '1d', '1h', '1m'. A partition
            key. Default: '1d'.
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        Path: Path to the series' single Parquet file.
    """
    return (
        Path(base_dir)
        / "bronze"
        / "group=bars"
        / f"source={source}"
        / f"symbol={symbol}"
        / f"interval={interval}"
        / "bars.parquet"
    )


def _write_frame_atomic(df: pd.DataFrame, path: Path) -> None:
    """Write a frame to ``path`` via a temp file and an atomic rename.

    A direct ``to_parquet`` that is interrupted mid-write leaves a truncated
    file in place, which then breaks every query over the lake. Writing to a
    temp sibling and renaming means the destination only ever holds a complete
    file. Mirrors the temp-then-rename protocol used by ``qde.compact``. Shared
    by the bars and series writers -- both keep one mutable file per series.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    df.to_parquet(tmp, engine="pyarrow")
    tmp.replace(path)  # os.replace: atomic overwrite on the same filesystem


def _watermark(path: Path) -> pd.Timestamp | None:
    """Return the max index value stored at ``path``, or None if absent/empty.

    The high-water mark for incremental loading, read from the stored rows
    themselves so it can never drift from the data. Shared by the bars and
    series watermarks.
    """
    if not path.exists():
        return None
    df = pd.read_parquet(path, engine="pyarrow")  # type: ignore[call-overload]
    if df.empty:
        return None
    return df.index.max()


def _upsert_frame(df: pd.DataFrame, path: Path) -> int:
    """Merge ``df`` into the file at ``path`` idempotently; return the row count.

    Rows are keyed by the frame's index. Existing rows are read, the incoming
    frame is concatenated on top, and clashes resolve last-write-wins so a fresh
    pull supersedes a prior value for the same key. The result is deduplicated,
    sorted, and written atomically. This is the idempotent partition overwrite
    shared by bars and series: a repeated or overlapping load converges to one
    row per key instead of accumulating duplicates.
    """
    if path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")  # type: ignore[call-overload]
        combined = pd.concat([existing, df])
    else:
        combined = df

    # keep="last": on a duplicate key the incoming row (concatenated last) wins.
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    _write_frame_atomic(combined, path)

    return len(combined)


def bars_watermark(
    symbol: str, source: str, interval: str = "1d", base_dir: str = "data"
) -> pd.Timestamp | None:
    """Return the last date stored for a bar series, or None if it is absent.

    This is the high-water mark for incremental loading: the next pull only
    needs data *after* this timestamp. It is read from the stored rows
    themselves rather than a separate ledger, so the watermark can never drift
    out of sync with the data it describes.

    Args:
        symbol (str): a ticker symbol, e.g. "BTCUSDT".
        source (str): the source, e.g. "binance".
        interval (str, optional): bar size. Default: '1d'.
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        pd.Timestamp | None: the newest stored date, or None if the series has
        no data yet.
    """
    return _watermark(_bars_path(symbol, source, interval, base_dir))


def upsert_bars(
    df: pd.DataFrame,
    symbol: str,
    source: str,
    interval: str = "1d",
    base_dir: str = "data",
) -> int:
    """Merge ``df`` into a bar series idempotently; return the stored row count.

    Rows are keyed by their UTC ``date`` index. Any existing rows are read, the
    incoming frame is concatenated on top, and clashes are resolved
    last-write-wins so the fresh pull supersedes a prior value for the same day.
    The result is deduplicated, sorted, and written atomically.

    This is the idempotent partition overwrite: re-running the same fetch, or a
    backfill whose range overlaps existing data, converges to exactly one row
    per date instead of accumulating duplicates. The series file is the unit of
    overwrite -- bars are one file per series, not one file per day.

    Args:
        df (pd.DataFrame): bars to merge, indexed by a UTC ``date`` index.
        symbol (str): a ticker symbol.
        source (str): the source.
        interval (str, optional): bar size. Default: '1d'.
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        int: number of rows in the resulting series file.
    """
    return _upsert_frame(df, _bars_path(symbol, source, interval, base_dir))


# Function to save the OHLCV data to Parquet
def save_ohlcv(
    symbol: str,
    source: str,
    start: str,
    end: str | None = None,
    interval: str = "1d",
    base_dir: str = "data",
) -> str:
    """Fetch a bar series from ``source`` and store it in the bronze bars lake.

    Thin wrapper over the unified loader and ``upsert_bars``: the fetched range
    is merged into any existing series rather than replacing it, so a repeated
    or overlapping save is idempotent instead of clobbering history.

    Args:
        symbol (str): a ticker symbol.
        source (str): the source.
        start (str): the time period to begin.
        end (str, optional): the time period to end. Default: now.
        interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        str: path to the series' Parquet file.
    """
    df = load_ohlcv(symbol, start=start, end=end, interval=interval, source=source)
    upsert_bars(df, symbol, source, interval, base_dir)

    return str(_bars_path(symbol, source, interval, base_dir))


# Local retrieve of the data
def load_ohlcv_local(
    symbol: str, source: str, interval: str = "1d", base_dir: str = "data"
) -> pd.DataFrame:
    """
    Read a saved Parquet file and return it as a pandas DataFrame.

    Args:
       symbol (str): a ticker symbol.
       source (str): a ticker source.
       interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.
       base_dir (str, optional): the base directory to retrieve the file. Default: 'data'.


    Returns:
          Clean DataFrame
    """
    # Check if path exits
    path = _bars_path(symbol, source, interval, base_dir)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # pandas-stubs' pyarrow-engine overload is too strict here; the call is valid.
    df = pd.read_parquet(path, engine="pyarrow")  # type: ignore[call-overload]

    return df


def update_ohlcv(symbol: str, source: str, interval: str = "1d", base_dir: str = "data") -> None:
    """Incrementally extend a stored bar series with any newer bars.

    Reads the series' watermark (its last stored date), fetches only bars after
    it, and upserts them. If the source has nothing newer (``NoNewData``), the
    series is already current and nothing is written. Any other failure -- an
    unknown/delisted symbol, an API error -- propagates so callers can count it
    as a failure rather than mistake a dead series for an up-to-date one.

    Args:
        symbol (str): a ticker symbol.
        source (str): the source.
        interval (str, optional): bar size. Default: '1d'.
        base_dir (str, optional): the lake root. Default: 'data'.

    Raises:
        FileNotFoundError: if the series has not been stored yet -- there is no
            watermark to advance from, so backfill or save it first.
        ValueError: if the loader fails for a real reason (unknown symbol, API
            error). Only ``NoNewData`` (a ``ValueError`` subclass) is swallowed
            as the benign already-current case.
    """
    watermark = bars_watermark(symbol, source, interval, base_dir)
    if watermark is None:
        raise FileNotFoundError(
            f"No stored series for {symbol!r}/{source!r}/{interval!r}; "
            "run a backfill or save_ohlcv first."
        )

    # Fetch from the day after the watermark: the last stored bar is already
    # held, and re-fetching it would only be deduplicated away.
    next_day = str((watermark + pd.Timedelta(days=1)).date())

    try:
        df_new = load_ohlcv(symbol, start=next_day, interval=interval, source=source)
    except NoNewData:
        # NoNewData means a successful fetch returned zero rows: for an
        # incremental pull that just means the series is already current. Only
        # this narrow case is treated as up-to-date -- a real failure (unknown
        # symbol, API error) raises a plain ValueError and propagates, so the
        # daily update counts it as failed instead of silently going stale.
        print(f"{symbol} already up to date through {watermark.date()}")
        return

    upsert_bars(df_new, symbol, source, interval, base_dir)


# Sql
def query(sql: str, base_dir: str = "data") -> pd.DataFrame:
    """
    Run SQL against the bars lake and return a DataFrame.

    Registers a single ``bars`` view over the partitioned bronze bars lake.
    Because it reads with ``hive_partitioning=true``, the partition keys
    (``source``, ``symbol``, ``interval``) come back as ordinary columns you
    can filter on, alongside the file's own columns (``date``, ``open``,
    ``high``, ``low``, ``close``, ``volume``). This replaces the old
    one-view-per-file scheme, where each file was its own table named
    ``<symbol>_<source>_<interval>``.

    A view is registered per group that actually has files (``bars``, ``series``),
    so ``FROM bars`` and ``FROM series`` both resolve, and an empty group simply
    has no view rather than erroring on an empty glob. Hive keys
    (``source``/``symbol``/``interval`` for bars; ``source``/``series_id`` for
    series) come back as filterable columns.

    Args:
        sql (str): a SQL query to execute, e.g.
            "SELECT date, close FROM bars WHERE symbol = 'BTCUSDT'".
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        pd.DataFrame: the query result.
    """
    con = duckdb.connect()
    # Render timestamps in UTC regardless of the host's locale, so query output
    # is deterministic across machines (a data platform serves clients anywhere).
    con.sql("SET TimeZone='UTC'")

    for group in ("bars", "series"):
        root = Path(base_dir) / "bronze" / f"group={group}"
        if not any(root.glob("**/*.parquet")):
            continue
        glob = (root / "**" / "*.parquet").as_posix()
        con.sql(
            f"CREATE OR REPLACE VIEW {group} AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
        )

    return con.sql(sql).df()


def list_bars_series(base_dir: str = "data") -> pd.DataFrame:
    """List the (source, symbol, interval) series present in the bars lake.

    Reads partition metadata straight from the lake, so callers never parse
    filenames. Returns an empty frame when no bars have landed yet.
    """
    root = Path(base_dir) / "bronze" / "group=bars"
    if not any(root.glob("**/*.parquet")):
        return pd.DataFrame(columns=["source", "symbol", "interval"])

    return query(
        "SELECT DISTINCT source, symbol, interval FROM bars "
        "ORDER BY source, symbol, interval",
        base_dir=base_dir,
    )


# --- series group (scalar time series) ---------------------------------------
#
# The series cousin of the bars storage above: one mutable file per series, no
# date partition (a series is one row per period), date kept as a column. See
# docs/schemas/series.md. The idempotent write and watermark are the shared
# helpers (_upsert_frame / _watermark), so a series behaves exactly like a bar
# series -- only the path and payload (date, value) differ.


def _series_path(
    series_id: str,
    source: str,
    base_dir: str = "data",
    metric: str | None = None,
) -> Path:
    """Build the bronze path for one scalar series.

    Hive-partitioned by ``source`` and ``series_id`` -- the keys you filter on.
    An optional ``metric`` partition separates the several scalars a single
    symbol can emit (a perp's ``funding_rate`` vs ``open_interest``); it is
    omitted for single-value sources like FRED, where ``series_id`` alone
    identifies the scalar.

    Args:
        series_id (str): the source-native series identifier, e.g. "CPIAUCSL".
        source (str): the source, e.g. "fred". A partition key.
        base_dir (str, optional): the lake root. Default: 'data'.
        metric (str | None, optional): a metric partition for multi-scalar
            sources. Default: None (omitted).

    Returns:
        Path: Path to the series' single Parquet file.
    """
    path = (
        Path(base_dir)
        / "bronze"
        / "group=series"
        / f"source={source}"
        / f"series_id={series_id}"
    )
    if metric is not None:
        path = path / f"metric={metric}"
    return path / "series.parquet"


def series_watermark(
    series_id: str, source: str, base_dir: str = "data", metric: str | None = None
) -> pd.Timestamp | None:
    """Return the last date stored for a scalar series, or None if absent.

    The high-water mark for incremental loading, read straight from the stored
    rows (twin of ``bars_watermark``).
    """
    return _watermark(_series_path(series_id, source, base_dir, metric))


def upsert_series(
    df: pd.DataFrame,
    series_id: str,
    source: str,
    base_dir: str = "data",
    metric: str | None = None,
) -> int:
    """Merge a scalar series into its file idempotently; return the row count.

    ``df`` is indexed by a UTC ``date`` index and carries a single ``value``
    column (see docs/schemas/series.md). Deduped by ``date`` last-write-wins and
    written atomically, exactly like ``upsert_bars`` -- a repeated or overlapping
    pull converges to one row per date.

    Args:
        df (pd.DataFrame): the series to merge, indexed by a UTC ``date`` index.
        series_id (str): the series identifier.
        source (str): the source.
        base_dir (str, optional): the lake root. Default: 'data'.
        metric (str | None, optional): metric partition for multi-scalar sources.

    Returns:
        int: number of rows in the resulting series file.
    """
    return _upsert_frame(df, _series_path(series_id, source, base_dir, metric))


def update_series(
    series_id: str, source: str, base_dir: str = "data", metric: str | None = None
) -> None:
    """Incrementally extend a stored scalar series with any newer observations.

    Reads the series' watermark, fetches only observations after it via the
    source's ingestor, and upserts them. ``NoNewData`` (nothing newer) is the
    benign already-current case; any other failure propagates so a real error (a
    bad key, an API outage) is counted as a failure rather than mistaken for
    up-to-date. Twin of ``update_ohlcv``.

    Note: this advances the latest-value series; it does not re-pull revisions to
    already-stored dates (macro data gets revised). Refreshing revisions is a
    full backfill, or the vintaged/ALFRED variant (see docs/schemas/series.md).

    Raises:
        FileNotFoundError: if the series has not been stored yet -- backfill first.
        ValueError: if the fetch fails for a real reason (bad key, API error).
            Only ``NoNewData`` is swallowed as the benign already-current case.
    """
    watermark = series_watermark(series_id, source, base_dir, metric)
    if watermark is None:
        raise FileNotFoundError(
            f"No stored series for {series_id!r}/{source!r}; run a backfill first."
        )

    # Fetch from the day after the watermark: the last stored observation is
    # already held, and re-fetching it would only be deduplicated away.
    next_day = str((watermark + pd.Timedelta(days=1)).date())

    # Lazy import: qde.ingest imports the registry (not storage), but keeping the
    # import local mirrors the loaders facade and avoids any import-time coupling.
    from qde.ingest import get_ingestor

    try:
        df_new = get_ingestor(source).load(series_id, start=next_day)
    except NoNewData:
        print(f"{series_id} already up to date through {watermark.date()}")
        return

    upsert_series(df_new, series_id, source, base_dir, metric)


def list_series(base_dir: str = "data") -> pd.DataFrame:
    """List the (source, series_id) series present in the series lake.

    Reads partition metadata straight from the lake, so callers never parse
    filenames. Returns an empty frame when no series have landed yet.
    """
    root = Path(base_dir) / "bronze" / "group=series"
    if not any(root.glob("**/*.parquet")):
        return pd.DataFrame(columns=["source", "series_id"])

    return query(
        "SELECT DISTINCT source, series_id FROM series ORDER BY source, series_id",
        base_dir=base_dir,
    )
