
import pandas as pd

from pathlib import Path
from qde.loaders import load_ohlcv


# Path helper function
def _ohlcv_path(
        symbol: str,
        source: str,
        interval: str = '1d',
        base_dir: str = 'data'
) -> Path:
    """
    Path helper function. Avoid repetition.

    Args:
        symbol (str): a ticker symbol.
        source (str): a ticker source.
        interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.
        base_dir (str, optional): the base directory to save the file. Default: 'data'.

    Returns:
        Path: Path to the saved file.
    """
    return Path(base_dir) / 'ohlcv' / f'{symbol}_{source}_{interval}.parquet'


# Function to save the OHLCV data to Parquet
def save_ohlcv(
        symbol: str,
        source: str,
        start: str,
        end: str | None = None,
        interval: str = '1d',
        base_dir: str = 'data'
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
    df =  load_ohlcv(symbol, start=start, end=end, interval=interval, source=source)

    # Create the directory if it doesn't exist. Build the file oath.
    path = _ohlcv_path(symbol, source, interval, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow')

    return str(path)


#
def load_ohlcv_local(
        symbol: str,
        source: str,
        interval: str = '1d',
        base_dir: str = 'data'
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
    path = _ohlcv_path(symbol, source, interval, base_dir)

    if not path.exists():
        raise FileNotFoundError(f'File not found: {path}')

    df = pd.read_parquet(path, engine='pyarrow')

    return df


# Update the file path on a regular basis
def update_ohlcv(
        symbol: str,
        source: str,
        interval: str = '1d',
        base_dir: str = 'data'
) -> None:

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
        print(f'{symbol} already up to date through {latest.date()}')
        return

    # Concatenate the data
    df = pd.concat([df_old, df_new])

    # Create the directory if it doesn't exist. Build the file oath.
    path = _ohlcv_path(symbol, source, interval, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow')









