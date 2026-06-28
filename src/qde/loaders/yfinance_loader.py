import pandas as pd
import yfinance as yf

def load_yfinance_ohlcv(symbol, start, end=None, interval='1d'):
    """ Load OHLCV data for a single symbol from Yahoo Finance, returning a cleaned
    DataFrame with flat lowercase columns and a UTC-aware index.

    Args:
        symbol (str): a ticker symbol
        start (str): start date, YYYY-MM-DD format
        end (str, optional): end date, YYYY-MM-DD format. Defaults to today if omitted.
        interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'

    Returns:
        DataFrame with columns: date, open, high, low, close, volume.
        Index by a UTC-aware DatetimeIndex named 'date'.

    Raises:
        ValueError: If empty DataFrame.
        """

    df  = yf.download(
        tickers=symbol,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=True,
    )

    # Guard against empty DataFrames
    if df.empty:
        raise ValueError(
            f"No df returned for symbol={symbol!r}, start={start!r}, end={end!r}, interval={interval!r}"
        )

    # Guard against multiindex update
    if isinstance(df.columns, pd.MultiIndex):
        df = df.droplevel(1, axis='columns')

    # LowerCasing for standardization
    df.columns = df.columns.str.lower()

    # Reordering the columns
    df = df[["open", "high", "low", "close", "volume"]]

    # Remove the name of the index
    df.columns.name = None

    # Timezone handling
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    else:
        df.index = df.index.tz_convert('UTC')

    df.index.name = "date"

    return df

