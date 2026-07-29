from pathlib import Path

import duckdb
import pandas as pd

from qde.loaders import load_ohlcv


# Path helper function
def _ohlcv_path(symbol: str, source: str, interval: str = "1d", base_dir: str = "data") -> Path:
    """
    Builds the file path for a given symbol/source/interval.
    The single source of truth for where files live.

    Args:
        symbol (str): a ticker symbol.
        source (str): a ticker source.
        interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.
        base_dir (str, optional): the base directory to save the file. Default: 'data'.

    Returns:
        Path: Path to the saved file.
    """
    path = Path(base_dir) / "ohlcv" / f"{symbol}_{source}_{interval}.parquet"
    return path


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


# Function to save the OHLCV data to Parquet
def save_ohlcv(
    symbol: str,
    source: str,
    start: str,
    end: str | None = None,
    interval: str = "1d",
    base_dir: str = "data",
) -> str:
    """
    Fetch data from a source and save it to Parquet.
    Thin wrapper around unified loader.
    Plus writes file.

    Args:
            symbol (str): a ticker symbol.
            source (str): a ticker source.
            start (str): the time period to begin.
            end (str): the time period to end.
            interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.
            base_dir (str, optional): the base directory to save the file. Default: 'data'.

    Returns:
        str: Path to the saved file.

    """
    # Call the unified loader to fetch the data
    df = load_ohlcv(symbol, start=start, end=end, interval=interval, source=source)

    # Create the directory if it doesn't exist. Build the file oath.
    path = _bars_path(symbol, source, interval, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")

    return str(path)


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


# Update the file path on a regular basis
def update_ohlcv(symbol: str, source: str, interval: str = "1d", base_dir: str = "data") -> None:

    # Load data in file
    df_old = load_ohlcv_local(symbol, source, interval, base_dir)

    # Retrieve the last day in the file
    latest = df_old.index.max()

    # Get the next day and convert to str for the loader
    next_day = str((latest + pd.Timedelta(days=1)).date())

    # Unified to fetch form the next_day upward
    try:
        df_new = load_ohlcv(symbol, start=next_day, source=source)
    except ValueError:
        print(f"{symbol} already up to date through {latest.date()}")
        return

    # Concatenate the data
    df = pd.concat([df_old, df_new])

    # Keep the last (most recent) timestamp
    df = df[~df.index.duplicated(keep="last")]

    # Create the directory if it doesn't exist. Build the file oath.
    path = _bars_path(symbol, source, interval, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")


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

    Args:
        sql (str): a SQL query to execute, e.g.
            "SELECT date, close FROM bars WHERE symbol = 'BTCUSDT'".
        base_dir (str, optional): the lake root. Default: 'data'.

    Returns:
        pd.DataFrame: the query result.
    """

    # Duckdb connection
    con = duckdb.connect()

    # One view over the whole bars lake; hive keys become filterable columns.
    bars_glob = (Path(base_dir) / "bronze" / "group=bars" / "**" / "*.parquet").as_posix()
    con.sql(
        "CREATE OR REPLACE VIEW bars AS "
        f"SELECT * FROM read_parquet('{bars_glob}', hive_partitioning=true)"
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
