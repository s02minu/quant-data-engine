import pandas as pd

from pathlib import Path
from qde.loaders import load_ohlcv


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

    """
    # Call the unified loader to fetch the data
    df =  load_ohlcv(symbol, start=start, end=end, interval=interval, source=source)

    # Create the directory if it doesn't exist. Build the file oath.
    path = Path(base_dir) / 'ohlcv' / f'{symbol}_{source}_{interval}.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow')

    return str(path)



