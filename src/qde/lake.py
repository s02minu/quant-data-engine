"""Query the R2 data lake with DuckDB.

The consumer side of serve-files-not-queries: DuckDB reads Parquet directly from
R2 over the network, with partition pruning and column pushdown, so only the
bytes a query touches are transferred. No server, no query compute to host.

Credentials are a read-only analysis token (separate from the VPS write token),
read from the environment:

    QDE_R2_ACCOUNT_ID, QDE_R2_READ_KEY_ID, QDE_R2_READ_SECRET, QDE_R2_BUCKET
"""

import os

import duckdb


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


def _load_local_env(path: str = "secrets/r2-read.env") -> None:
    """Load KEY=VALUE lines from a local env file into the environment.

    A CLI convenience so `python -m qde.lake` works in any shell without
    sourcing first. Existing environment variables are not overridden.
    """
    if not os.path.exists(path):
        return
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


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
