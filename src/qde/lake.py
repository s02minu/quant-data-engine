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
    kind: str = "*", symbol: str = "*", date: str = "*", bucket: str | None = None
) -> str:
    """Build a r2:// glob selecting bronze partitions.

    Each argument narrows a partition key; the default "*" matches all. A
    narrower glob prunes to fewer files, so DuckDB transfers less.
    """
    bucket = bucket or os.environ.get("QDE_R2_BUCKET", "qde-lake")
    return (
        f"r2://{bucket}/bronze/group=microstructure/source=binance/"
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
    source: str = "*", series_id: str = "*", bucket: str | None = None
) -> str:
    """Build an r2:// glob selecting scalar series.

    The series twin of ``bars_glob``. Series are grouped by shape, not partitioned
    by date -- one file per (source, series_id) -- so the keys are source and
    series_id. The trailing ``**`` absorbs the optional ``metric=`` partition that
    multi-scalar sources add (a perp's ``funding_rate`` vs ``open_interest``) while
    still matching single-scalar sources like FRED, whose file sits one level
    shallower. Each argument narrows a key; the default "*" matches all.
    """
    bucket = bucket or os.environ.get("QDE_R2_BUCKET", "qde-lake")
    return (
        f"r2://{bucket}/bronze/group=series/source={source}/"
        f"series_id={series_id}/**/*.parquet"
    )


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
    # against R2 and the local lake. Guarded like the kinds below: before the FRED
    # deploy lands series in R2 the glob is empty, so skip the view rather than
    # break every query.
    with contextlib.suppress(Exception):
        con.execute(
            f"CREATE OR REPLACE VIEW series AS "
            f"SELECT * FROM read_parquet('{series_glob()}', hive_partitioning=true)"
        )

    for kind in _MICROSTRUCTURE_KINDS:
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW {kind} AS "
                f"SELECT * FROM read_parquet('{bronze_glob(kind=kind)}', hive_partitioning=true)"
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
        SELECT kind, symbol, count(*) AS rows
        FROM read_parquet('{glob}', hive_partitioning = true)
        GROUP BY kind, symbol
        ORDER BY kind, symbol
        """
    ).df()
    print(result.to_string(index=False))
