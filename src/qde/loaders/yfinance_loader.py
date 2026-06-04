import pandas as pd
import yfinance as yf

def load_ohlcv(symbol, start, end=None, interval='1d'):
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

    data  = yf.download(
        tickers=symbol,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=True,
    )

    # Guard against empty DataFrames
    if data.empty:
        raise ValueError(
            f"No data returned for symbol={symbol!r}, start={start!r}, end={end!r}, interval={interval!r}"
        )

    # Guard against multiindex update
    if isinstance(data.columns, pd.MultiIndex):
        data = data.droplevel(1, axis='columns')

    # LowerCasing for standardization
    data.columns = data.columns.str.lower()

    # Reordering the columns
    data = data[["open", "high", "low", "close", "volume"]]

    # Timezone handling
    if data.index.tz is None:
        data.index = data.index.tz_localize('UTC')
    else:
        data.index = data.index.tz_convert('UTC')

    data.index.name = 'date'

    return data

