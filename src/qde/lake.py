"""Query the R2 data lake with DuckDB.

The consumer side of serve-files-not-queries: DuckDB reads Parquet directly from
R2 over the network, with partition pruning and column pushdown, so only the
bytes a query touches are transferred. No server, no query compute to host.

Credentials are a read-only analysis token (separate from the VPS write token),
read from the environment:

    QDE_R2_ACCOUNT_ID, QDE_R2_READ_KEY_ID, QDE_R2_READ_SECRET, QDE_R2_BUCKET
"""

import contextlib
import os

import duckdb
import pandas as pd

from qde.env import load_env_file


def connect() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection wired to read the R2 lake.

    Loads httpfs and registers an R2 secret from environment credentials. The
    values are bound as parameters, so the keys never appear in a SQL string.
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        "CREATE OR REPLACE SECRET r2 (TYPE r2, KEY_ID ?, SECRET ?, ACCOUNT_ID ?)",
        [
            os.environ["QDE_R2_READ_KEY_ID"],
            os.environ["QDE_R2_READ_SECRET"],
            os.environ["QDE_R2_ACCOUNT_ID"],
        ],
    )
    return con


def open_lake() -> duckdb.DuckDBPyConnection:
    """Load local credentials (if present) and return a connected lake handle.

    Convenience for interactive exploration: `from qde.lake import open_lake`.
    """
    _load_local_env()
    return connect()


def bronze_glob(
    source: str = "*",
    kind: str = "*",
    symbol: str = "*",
    date: str = "*",
    bucket: str | None = None,
) -> str:
    """Build a r2:// glob selecting bronze microstructure partitions.

    Each argument narrows a partition key; the default "*" matches all. A
    narrower glob prunes to fewer files, so DuckDB transfers less. ``source``
    leads (mirroring ``bars_glob``/``series_glob``) so the microstructure lake
    spans venues: Binance and Coinbase share a ``symbol=`` partition, with
    ``source=`` distinguishing the USD/USDT book -- the cross-venue basis signal.
    A read over the multi-venue glob must pass ``union_by_name=true`` because the
    per-venue bronze schemas differ (Coinbase carries extra ticker columns and
    string/ISO-8601 fields), which is what the ``query`` views do.
    """
    bucket = bucket or os.environ.get("QDE_R2_BUCKET", "qde-lake")
    return (
        f"r2://{bucket}/bronze/group=microstructure/source={source}/"
        f"kind={kind}/symbol={symbol}/date={date}/*.parquet"
    )


def bars_glob(
    source: str = "*", symbol: str = "*", interval: str = "*", bucket: str | None = None
) -> str:
    """Build an r2:// glob selecting bars series.

    The bars twin of ``bronze_glob``. Bars are grouped by shape, not partitioned
    by date -- one file per (source, symbol, interval) -- so the keys are source,
    symbol, and interval, with no kind or date. Each argument narrows a key; the
    default "*" matches all.
    """
    bucket = bucket or os.environ.get("QDE_R2_BUCKET", "qde-lake")
    return (
        f"r2://{bucket}/bronze/group=bars/source={source}/"
        f"symbol={symbol}/interval={interval}/*.parquet"
    )


def series_glob(
    source: str = "*",
    series_id: str = "*",
    metric: str | None = None,
    bucket: str | None = None,
) -> str:
    """Build an r2:// glob selecting scalar series at a single partition depth.

    The series twin of ``bars_glob``. Series are grouped by shape, not partitioned
    by date -- one file per (source, series_id) -- so the keys are source and
    series_id. Multi-scalar sources add a ``metric=`` level (a perp's
    ``funding_rate`` vs ``open_interest``; CFTC COT's trader categories).

    ``metric`` selects the depth: ``None`` (default) globs the *flat* single-value
    files (FRED, CBOE); a value (``"*"`` for all) globs the *metric* partition.
    The two depths are globbed separately on purpose -- DuckDB rejects a single
    glob spanning both hive depths, so ``query`` unions them (see there). Each
    argument narrows a key; the default "*" matches all.
    """
    bucket = bucket or os.environ.get("QDE_R2_BUCKET", "qde-lake")
    base = f"r2://{bucket}/bronze/group=series/source={source}/series_id={series_id}/"
    if metric is None:
        return base + "series.parquet"  # flat: FRED, CBOE
    return base + f"metric={metric}/series.parquet"  # metric partition: COT, perps


def events_glob(source: str = "*", calendar: str = "*", bucket: str | None = None) -> str:
    """Build an r2:// glob selecting events calendars.

    The events twin of ``bars_glob``. Events are grouped by shape, one file per
    (source, calendar) -- a calendar holds many series' releases (docs/schemas/
    events.md) -- so the keys are source and calendar, with no date. Each argument
    narrows a key; the default "*" matches all.
    """
    bucket = bucket or os.environ.get("QDE_R2_BUCKET", "qde-lake")
    return (
        f"r2://{bucket}/bronze/group=events/source={source}/"
        f"calendar={calendar}/events.parquet"
    )


# Gold marts (dbt-materialized Parquet), by view name -> path relative to the
# bucket root. Each is a single mart file the nightly `dbt build` rewrites and
# `sync.publish_gold` ships. Extend this as new marts land (series/events gold).
_GOLD_MARTS = {
    "fct_bars_daily": "gold/group=bars/mart=fct_bars_daily/*.parquet",
    "fct_series_features": "gold/group=series/mart=fct_series_features/*.parquet",
    "fct_events_revisions": "gold/group=events/mart=fct_events_revisions/*.parquet",
    "dim_sources": "gold/dim_sources/*.parquet",
}


def gold_glob(mart: str, bucket: str | None = None) -> str:
    """Build an r2:// glob selecting a named gold mart.

    Gold is the medallion layer above bronze (``gold/`` sibling of ``bronze/``),
    holding the dbt-materialized analytics marts. Unlike the bronze groups these
    are named products, so the glob is looked up by mart name (``fct_bars_daily``,
    ``dim_sources``) rather than built from partition keys.
    """
    bucket = bucket or os.environ.get("QDE_R2_BUCKET", "qde-lake")
    return f"r2://{bucket}/{_GOLD_MARTS[mart]}"


_MICROSTRUCTURE_KINDS = ("trades", "depth", "book_ticker", "snapshot", "gaps", "session")


def query(sql: str) -> pd.DataFrame:
    """Run SQL against the R2 lake with named views already registered.

    The R2 counterpart of ``qde.storage.query``: a query reads like plain SQL
    against tables -- ``SELECT ... FROM bars WHERE symbol = 'BTCUSDT'`` -- rather
    than spelling out ``read_parquet('r2://.../*.parquet', hive_partitioning=true)``.
    The same SQL therefore runs against the local lake (``storage.query``) and
    the R2 lake (here).

    Registers a ``bars`` view, a ``series`` view, plus one view per microstructure
    kind (``trades``, ``depth``, ``book_ticker``, ...), one per kind because each
    kind has its own schema. Hive partition keys come back as ordinary, filterable
    columns. Views are lazy: creating them transfers nothing; a query only reads
    the partitions its ``WHERE`` clause selects.
    """
    con = open_lake()

    con.execute(
        f"CREATE OR REPLACE VIEW bars AS "
        f"SELECT * FROM read_parquet('{bars_glob()}', hive_partitioning=true)"
    )

    # A ``series`` view mirrors the local ``storage.query`` so the same SQL runs
    # against R2 and the local lake. Series live at a *mixed* partition depth --
    # flat single-value files (FRED, CBOE) plus multi-metric ``metric=`` partitions
    # (CFTC COT, perps) -- and DuckDB rejects a single glob spanning both, so union
    # the two depths with UNION ALL BY NAME (metric=NULL on the flat side). Each
    # side is probed independently: an empty glob raises, so a depth with no files
    # yet (e.g. no metric sources before COT lands) is simply skipped, and before
    # any series reach R2 the view is skipped entirely rather than breaking queries.
    reads = []
    for glob in (series_glob(), series_glob(metric="*")):
        with contextlib.suppress(Exception):
            con.execute(f"SELECT 1 FROM read_parquet('{glob}', hive_partitioning=true) LIMIT 1")
            reads.append(f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)")
    if reads:
        con.execute("CREATE OR REPLACE VIEW series AS " + " UNION ALL BY NAME ".join(reads))

    # An ``events`` view mirrors the local ``storage.query``. Events sit at a uniform
    # depth (source/calendar), one file per calendar, so a single glob suffices --
    # no union. Skipped before any calendar reaches R2 (an empty glob raises) rather
    # than breaking queries.
    with contextlib.suppress(Exception):
        con.execute(
            "CREATE OR REPLACE VIEW events AS "
            f"SELECT * FROM read_parquet('{events_glob()}', hive_partitioning=true)"
        )

    # Gold marts: one view per dbt mart (fct_bars_daily, dim_sources). Each is
    # skipped until it reaches R2 (an empty glob raises), so `FROM fct_bars_daily`
    # runs the same query locally (storage.query) and over R2 once published.
    for mart in _GOLD_MARTS:
        with contextlib.suppress(Exception):
            con.execute(
                f"CREATE OR REPLACE VIEW {mart} AS "
                f"SELECT * FROM read_parquet('{gold_glob(mart)}', hive_partitioning=true)"
            )

    for kind in _MICROSTRUCTURE_KINDS:
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW {kind} AS SELECT * FROM read_parquet("
                f"'{bronze_glob(kind=kind)}', hive_partitioning=true, union_by_name=true)"
            )
        except Exception:
            # A kind with no data yet has no files to glob; skip its view rather
            # than fail the whole query.
            continue

    return con.execute(sql).df()


def _load_local_env(path: str = "secrets/r2-read.env") -> None:
    """Load KEY=VALUE lines from a local env file into the environment.

    A CLI convenience so `python -m qde.lake` works in any shell without
    sourcing first. Delegates to the shared BOM-tolerant loader (`qde.env`) so a
    PowerShell-written UTF-16/BOM secrets file is read correctly.
    """
    load_env_file(path)


if __name__ == "__main__":
    _load_local_env()
    con = connect()
    glob = bronze_glob()
    result = con.execute(
        f"""
        SELECT source, kind, symbol, count(*) AS rows
        FROM read_parquet('{glob}', hive_partitioning = true, union_by_name = true)
        GROUP BY source, kind, symbol
        ORDER BY source, kind, symbol
        """
    ).df()
    print(result.to_string(index=False))
