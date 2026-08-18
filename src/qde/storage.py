import os
from pathlib import Path

import duckdb
import pandas as pd

from qde.loaders import NoNewData, load_ohlcv
from qde.log import get_logger

log = get_logger(__name__)


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


def _write_frame_atomic(df: pd.DataFrame, path: Path, index: bool = True) -> None:
    """Write a frame to ``path`` via a temp file and an atomic rename.

    A direct ``to_parquet`` that is interrupted mid-write leaves a truncated
    file in place, which then breaks every query over the lake. Writing to a
    temp sibling and renaming means the destination only ever holds a complete
    file. Mirrors the temp-then-rename protocol used by ``qde.compact``. Shared
    by the bars, series, and events writers.

    ``index`` is written for bars/series (their index *is* the meaningful ``date``
    key) but not for events, whose rows are keyed by ``(event_id, revision_seq)``
    columns and carry only a positional index worth discarding.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    try:
        df.to_parquet(tmp, engine="pyarrow", index=index)
        # The rename is atomic, but atomicity is not durability: without this the
        # kernel may carry out the rename while the temp file's *contents* are still
        # in the page cache, so a power loss can leave a complete-looking file with
        # missing bytes at the destination. Flushing before the swap is what makes
        # the "crash-safe" claim above actually true.
        # "rb+", not "rb": Windows refuses to flush a handle opened read-only.
        with open(tmp, "rb+") as fh:
            os.fsync(fh.fileno())
        tmp.replace(path)  # os.replace: atomic overwrite on the same filesystem
    finally:
        # A failed write must not leave a half-file behind for the next run to trip
        # over. Named with a leading dot and a .tmp suffix so no lake glob would
        # match it either way, but cleaning up is cheaper than relying on that.
        tmp.unlink(missing_ok=True)


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


def _warn_on_schema_drift(existing: pd.DataFrame, incoming: pd.DataFrame, path: Path) -> None:
    """Say something when a pull's columns stop matching what is already stored.

    ``concat`` reconciles mismatched columns by filling NaN, which is exactly the
    wrong behaviour to have happen quietly. If a source drops a field, every
    re-fetched row overwrites a real value with NaN — last-write-wins means the
    incoming row is the one that survives — and the file keeps its full column list,
    so nothing downstream looks broken until someone notices a column has gone
    hollow for recent dates. A new column is usually legitimate growth; a
    disappearing one almost never is. Both are reported, neither is fatal: refusing
    the write would turn a cosmetic API change into an outage.
    """
    lost = [c for c in existing.columns if c not in incoming.columns]
    gained = [c for c in incoming.columns if c not in existing.columns]
    if lost or gained:
        log.warning(
            "schema_drift",
            path=str(path),
            columns_missing_from_pull=lost,
            new_columns=gained,
            note="missing columns become NaN for every overwritten row",
        )


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
        _warn_on_schema_drift(existing, df, path)
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


def update_ohlcv(
    symbol: str,
    source: str,
    interval: str = "1d",
    base_dir: str = "data",
    violations: list | None = None,
) -> None:
    """Incrementally extend a stored bar series with any newer bars.

    Reads the series' watermark (its last stored date), fetches only bars after
    it, and upserts them. If the source has nothing newer (``NoNewData``), the
    series is already current and nothing is written. Any other failure -- an
    unknown/delisted symbol, an API error -- propagates so callers can count it
    as a failure rather than mistake a dead series for an up-to-date one.

    No bar can exist for a day that has not started yet, so if the watermark is
    already at or past today (UTC) the next-day start would land in the future --
    skipped as the same benign already-current case, *without* a network call.
    This matters beyond saving a request: some venues (observed on Coinbase)
    reject a future ``start`` outright rather than returning an empty page like
    the rest, which would otherwise surface as a spurious daily-update failure
    whenever this function runs more than once on the same UTC day (e.g. a manual
    re-run after the nightly cron already advanced the watermark to today).

    Args:
        symbol (str): a ticker symbol.
        source (str): the source.
        interval (str, optional): bar size. Default: '1d'.
        base_dir (str, optional): the lake root. Default: 'data'.
        violations (list, optional): a list the caller can pass to receive any
            :class:`~qde.checks.Violation` the fetched frame trips before it is
            stored. **The frame is stored either way.** Bronze is the replay log,
            so rejecting a suspect frame would destroy the very evidence needed to
            diagnose it -- the data lands intact and the violation records what we
            think of it, in the quality tables, where a verdict can be revised
            without touching the data.

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
    next_day = watermark + pd.Timedelta(days=1)
    today = pd.Timestamp.now(tz="UTC").normalize()
    if next_day > today:
        print(f"{symbol} already up to date through {watermark.date()}")
        return

    try:
        df_new = load_ohlcv(symbol, start=str(next_day.date()), interval=interval, source=source)
    except NoNewData:
        # NoNewData means a successful fetch returned zero rows: for an
        # incremental pull that just means the series is already current. Only
        # this narrow case is treated as up-to-date -- a real failure (unknown
        # symbol, API error) raises a plain ValueError and propagates, so the
        # daily update counts it as failed instead of silently going stale.
        print(f"{symbol} already up to date through {watermark.date()}")
        return

    if violations is not None:
        # Verified *before* the upsert, because this is the only moment the frame
        # exists as the source sent it — once merged, a mismapped column or an
        # epoch-unit error is indistinguishable from history. Recorded, never
        # enforced: see the `violations` arg for why bronze still takes the frame.
        from qde.verify import verify_frame

        violations.extend(
            verify_frame(
                df_new,
                group="bars",
                source=source,
                series_id=symbol,
                start=str(next_day.date()),
                interval=interval,
            )
        )

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

    bronze = Path(base_dir) / "bronze"

    # bars: a uniform partition depth (source/symbol/interval), so one glob.
    bars_root = bronze / "group=bars"
    if any(bars_root.glob("**/*.parquet")):
        glob = (bars_root / "**" / "*.parquet").as_posix()
        con.sql(
            "CREATE OR REPLACE VIEW bars AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
        )

    # series: a *mixed* partition depth. Single-value sources (FRED, CBOE) sit at
    # source/series_id/series.parquet; multi-metric sources (CFTC COT, perps) add
    # a metric= level (docs/schemas/series.md). DuckDB rejects mixed hive depth
    # under one glob ("Hive partition mismatch"), so union a flat glob with a
    # metric glob and let UNION ALL BY NAME fill metric=NULL for the flat side.
    series_root = bronze / "group=series"
    reads = [
        f"SELECT * FROM read_parquet('{(series_root / depth).as_posix()}', hive_partitioning=true)"
        for depth in ("*/*/series.parquet", "*/*/*/series.parquet")
        if any(series_root.glob(depth))
    ]
    if reads:
        con.sql("CREATE OR REPLACE VIEW series AS " + " UNION ALL BY NAME ".join(reads))

    # events: a uniform partition depth (source/calendar), one events.parquet per
    # calendar (docs/schemas/events.md) -- so one glob, like bars.
    events_root = bronze / "group=events"
    if any(events_root.glob("**/*.parquet")):
        glob = (events_root / "**" / "*.parquet").as_posix()
        con.sql(
            "CREATE OR REPLACE VIEW events AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
        )

    # gold: the dbt-materialized marts (a medallion layer above bronze). One view
    # per mart at its known path, so `FROM fct_bars_daily` runs the same SQL against
    # the local lake here and against R2 in qde.lake. A mart not built yet is
    # skipped rather than erroring on an empty glob.
    gold = Path(base_dir) / "gold"
    gold_marts = {
        "fct_bars_daily": gold / "group=bars" / "mart=fct_bars_daily",
        "dim_sources": gold / "dim_sources",
    }
    for mart, mart_dir in gold_marts.items():
        if any(mart_dir.glob("*.parquet")):
            mart_glob = (mart_dir / "*.parquet").as_posix()
            con.sql(
                f"CREATE OR REPLACE VIEW {mart} AS "
                f"SELECT * FROM read_parquet('{mart_glob}', hive_partitioning=true)"
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

    For a multi-metric series (CFTC COT, perps) there is no flat
    ``series_id/series.parquet`` -- the data lives under ``metric=`` partitions.
    When no ``metric`` is given and the flat file is absent, the watermark is
    taken across the metric partitions: every metric of a market shares the same
    report dates, so their max is the series' high-water mark. This lets the
    per-series incremental update advance a multi-metric source with one fetch.
    """
    path = _series_path(series_id, source, base_dir, metric)
    wm = _watermark(path)
    if wm is not None or metric is not None:
        return wm

    # No flat file and no metric requested: this may be a multi-metric series.
    parent = path.parent  # .../series_id=<id>/
    if not parent.exists():
        return None
    marks = [_watermark(p) for p in parent.glob("metric=*/series.parquet")]
    present = [m for m in marks if m is not None]
    return max(present) if present else None


def upsert_series_frame(
    df: pd.DataFrame, series_id: str, source: str, base_dir: str = "data"
) -> int:
    """Upsert a series frame that is either single-value or multi-metric.

    The bridge between an ingestor's returned frame and the ``series`` storage.
    A frame carrying the single column ``value`` is one scalar series (FRED,
    CBOE) and is written flat. Any other columns are treated as **metrics** --
    one file per column under a ``metric=`` partition -- which is the multi-scalar
    shape (CFTC COT's trader-category positions, a perp's funding/OI). The metric
    name is simply the column name, so an ingestor declares its metrics by naming
    its columns. Every write goes through the same idempotent ``upsert_series``.

    Returns:
        int: total rows written across all metric files (or the single file).
    """
    if list(df.columns) == ["value"]:
        return upsert_series(df, series_id, source, base_dir)

    total = 0
    for metric in df.columns:
        one = df[[metric]].rename(columns={metric: "value"})
        total += upsert_series(one, series_id, source, base_dir, metric=str(metric))
    return total


def load_series_local(
    series_id: str, source: str, base_dir: str = "data"
) -> pd.DataFrame:
    """Read a stored scalar series back as the *wide* frame an ingestor returns.

    The exact inverse of :func:`upsert_series_frame`, and deliberately so: that
    function splits a wide frame into one file per ``metric=`` partition, and a
    comparison against a fresh fetch has to put it back together to compare like
    with like. A single-value source (FRED, CBOE) round-trips to one ``value``
    column; a multi-metric one (CFTC COT's trader categories) to one column per
    metric, named as the partition is.

    Raises:
        FileNotFoundError: nothing stored for this series.
    """
    root = (
        Path(base_dir)
        / "bronze"
        / "group=series"
        / f"source={source}"
        / f"series_id={series_id}"
    )
    flat = root / "series.parquet"
    if flat.exists():
        return pd.read_parquet(flat)

    frames = {}
    for part in sorted(root.glob("metric=*/series.parquet")):
        metric = part.parent.name.split("=", 1)[1]
        one = pd.read_parquet(part)
        if "value" in one.columns:
            frames[metric] = one["value"]
    if not frames:
        raise FileNotFoundError(f"No stored series for {series_id!r}/{source!r}")

    # `join="outer"`: metrics of one series share report dates in practice, but a
    # metric added later legitimately starts later, and an inner join would silently
    # truncate every other metric to its history.
    # `sort=False` stated explicitly: pandas 4 changes the default for a concat of
    # DatetimeIndexes, and the ordering is established by the sort_index() below
    # regardless — pinning it here keeps that a deliberate choice rather than a
    # behaviour that silently flips on an upgrade.
    return pd.concat(frames, axis=1, join="outer", sort=False).sort_index()


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
    series_id: str,
    source: str,
    base_dir: str = "data",
    metric: str | None = None,
    violations: list | None = None,
) -> None:
    """Incrementally extend a stored scalar series with any newer observations.

    Reads the series' watermark, fetches only observations after it via the
    source's ingestor, and upserts them. ``NoNewData`` (nothing newer) is the
    benign already-current case; any other failure propagates so a real error (a
    bad key, an API outage) is counted as a failure rather than mistaken for
    up-to-date. Twin of ``update_ohlcv``.

    Works for single-value and multi-metric sources alike: one fetch returns the
    whole frame (a single ``value`` column, or one column per metric), and
    ``upsert_series_frame`` writes it flat or splits it across ``metric=``
    partitions. A multi-metric market's metrics share the same report dates, so a
    single watermark and a single fetch keep them all current.

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

    if violations is not None:
        # Same reasoning as `update_ohlcv`: the frame is only checkable as the
        # source sent it. Wired later than bars were, which meant the series group
        # had a written contract that nothing in the running system ever applied.
        from qde.verify import verify_frame

        violations.extend(
            verify_frame(
                df_new,
                group="series",
                source=source,
                series_id=series_id,
                start=next_day,
            )
        )

    upsert_series_frame(df_new, series_id, source, base_dir)


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


# --- events group (bitemporal scheduled releases) ----------------------------
#
# The economic-calendar cousin of bars/series. An event is a scheduled, revisable
# release (docs/schemas/events.md): sparse and tiny, so one mutable file per
# *calendar* holds many series' events side by side, with the reference/observed
# dates as columns rather than partitions (same small-files reasoning as series).
# Rows are keyed by (event_id, revision_seq) -- one per release and per revision --
# not by a date index, so the upsert dedups on those columns instead of the index.


def _events_path(source: str, calendar: str, base_dir: str = "data") -> Path:
    """Build the bronze path for one events calendar.

    Hive-partitioned by ``source`` and ``calendar`` -- the keys you filter on.
    A calendar is a named slice (``us_macro``, ``earnings``) so unrelated event
    streams live in separate files.

    Args:
        source (str): the calendar's source, e.g. "fredcal". A partition key.
        calendar (str): the named calendar, e.g. "us_macro". A partition key.
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        Path: Path to the calendar's single Parquet file.
    """
    return (
        Path(base_dir)
        / "bronze"
        / "group=events"
        / f"source={source}"
        / f"calendar={calendar}"
        / "events.parquet"
    )


def upsert_events(
    df: pd.DataFrame, source: str, calendar: str, base_dir: str = "data"
) -> int:
    """Merge events into a calendar file idempotently; return the stored row count.

    Rows are keyed by ``(event_id, revision_seq)`` -- one row per release and per
    revision -- rather than by a date index, so this dedups on those columns
    (last-write-wins) instead of reusing the index-keyed ``_upsert_frame``. A
    re-pull of the full vintage history (how the calendar is refreshed, since a
    revision is a new row for an existing event) therefore converges to exactly
    one row per (event, revision) instead of accumulating duplicates. Ordered by
    ``scheduled_ts`` then ``event_id`` then ``revision_seq`` and written atomically.

    Args:
        df (pd.DataFrame): events to merge, in the schema shape (docs/schemas/events.md).
        source (str): the calendar source, e.g. "fredcal".
        calendar (str): the named calendar, e.g. "us_macro".
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        int: number of rows in the resulting calendar file.
    """
    path = _events_path(source, calendar, base_dir)
    if path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")  # type: ignore[call-overload]
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df.reset_index(drop=True)

    combined = (
        combined.drop_duplicates(subset=["event_id", "revision_seq"], keep="last")
        .sort_values(["scheduled_ts", "event_id", "revision_seq"])
        .reset_index(drop=True)
    )
    _write_frame_atomic(combined, path, index=False)

    return len(combined)


def update_events(
    series_id: str,
    source: str,
    calendar: str,
    start: str,
    base_dir: str = "data",
    violations: list | None = None,
) -> int:
    """Full-refresh one series' release history into its calendar file.

    Unlike ``update_series`` (watermark-advanced), events are re-pulled in *full*
    from ``start``: a revision is a new ``(event_id, revision_seq)`` row for an
    already-stored reference period, which a watermark advanced past that period's
    date would never re-fetch. The re-pull is idempotent (``upsert_events`` dedups)
    and calendars are tiny, so a full refresh is both correct and cheap.

    Raises:
        NoNewData: the series returned nothing from ``start`` -- the benign empty
            case, for the caller to treat as a no-op (mirrors the series/bars
            update contract).
        ValueError: a real fetch failure (bad key, API error) propagates.
    """
    # Lazy import mirrors update_series -- keeps storage from importing the ingest
    # package at module load.
    from qde.ingest import get_ingestor

    df = get_ingestor(source).load(series_id, start)

    if violations is not None:
        # The events contract is the strictest of the three — bitemporal ordering,
        # a unique (event_id, revision_seq) key, and a contiguous revision run — and
        # until now none of it ran outside the tests. `start` is not passed: this is
        # a deliberate full re-pull from the ALFRED era, so rows before it are the
        # point rather than a range violation.
        from qde.verify import verify_frame

        violations.extend(
            verify_frame(df, group="events", source=source, series_id=series_id)
        )

    return upsert_events(df, source, calendar, base_dir)


def count_events(source: str, calendar: str, base_dir: str = "data") -> int:
    """Return the number of rows in a calendar file, or 0 if it does not exist.

    All of a calendar's series share one ``events.parquet``, so ``upsert_events``
    returns the *cumulative* file count; a caller that wants the rows a single
    series contributed reads this before/after (the delta). Cheap — calendars are
    tiny.
    """
    path = _events_path(source, calendar, base_dir)
    if not path.exists():
        return 0
    return len(pd.read_parquet(path, engine="pyarrow"))  # type: ignore[call-overload]


def list_events(base_dir: str = "data") -> pd.DataFrame:
    """List the (source, calendar) calendars present in the events lake.

    Reads partition metadata straight from the lake, so callers never parse
    filenames. Returns an empty frame when no events have landed yet.
    """
    root = Path(base_dir) / "bronze" / "group=events"
    if not any(root.glob("**/*.parquet")):
        return pd.DataFrame(columns=["source", "calendar"])

    return query(
        "SELECT DISTINCT source, calendar FROM events ORDER BY source, calendar",
        base_dir=base_dir,
    )
