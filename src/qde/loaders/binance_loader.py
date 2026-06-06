import pandas as pd
import requests

def load_binance_ohlcv(symbol: str, interval: str="1d", limit: int=1000) -> pd.DataFrame:
    """ Load OHLCV data for a single symbol from binance through
    an API request. Returning a cleaned utc aware index.


        Args:
            symbol (str): a ticker symbol.
            interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.
            limit (int, optional): the number of rows to return. Defaults to 1000.

        Returns:
            DataFrame with columns: date, open, high, low, close, volume.
            Index by a UTC-aware DatetimeIndex named 'date'.

        Raises:
            ValueError: If no response and if empty DataFrame.
            """

    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    response = requests.get(url, params=params)

    # Fail test Guard for no response from binance
    if response.status_code != 200:
        raise ValueError(
            f"Binance API error {response.status_code}: {response.text}"
        )

    # Fail test Guard for no data returned but request successful
    data = response.json()
    if not data:
        raise ValueError(
        f"No data returned for symbol={symbol!r}, interval={interval!r}, limit={limit!r}"
        )

    df = pd.DataFrame(data,
                      columns=["kline_open", "open", "high", "low", "close", "volume",
                               "kline_close", "quote_volume", "num_trades",
                               "taker_buy_volume", "taker_buy_quote_volume", "unused"])

    # Convert the str to numeric
    numeric_columns = ["open", "high", "low", "close", "volume",
                       "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"]

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    # Convert from epoch ms to utc aware datetime and se as index
    df.index = pd.to_datetime(df["kline_open"], unit="ms", utc=True)
    df.index.name = "date"

    # Select desired column only
    df = df[["open", "high", "low", "close", "volume"]]

    return df