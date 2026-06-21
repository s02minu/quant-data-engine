import pandas as pd
import requests

def load_kraken_ohlcv(symbol, start, end=None, interval="1d") -> pd.DataFrame:
    """ Load OHLCV data for a single symbol from Kraken, returning a
        cleaned DataFrame with flat lowercase columns and a UTC-aware index.

        Args:
            symbol (str): a ticker symbol.
            start (str): the time period to begin.
            end (str): the time period to end.
            interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.
            limit (int, optional): maximum number of candles to return. Defaults to 1000.

        Returns:
            DataFrame with columns: date, open, high, low, close, volume.
            Index by a UTC-aware DatetimeIndex named 'date'.

        Raises:
            ValueError: If the API returns a non-200 status or an empty response.
            """

    # Convert start to epoch ms
    since = int(pd.Timestamp(start, tz="UTC").timestamp())

    # Kraken Interval mapping
    interval_map = {
        "1d": 1440,
        "1h": 60,
        "15m": 15,
        "5m": 5,
        "1m": 1,
    }
    kraken_interval = interval_map.get(interval)
    if kraken_interval is None:
        raise ValueError(f"Unsupported interval: {interval!r}")

    # Get the request
    url = "https://api.kraken.com/0/public/OHLC"

    params = {
        "pair": symbol,
        "interval": kraken_interval,
        "since": since,
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Guard against error
    if data["error"]:
        raise ValueError(f"Kraken API error: {data['error']}")

    if not data["result"]:
        raise ValueError()

    result = data["result"]
    pair_key = [k for k in result if k != "last"][0]
    candles = result[pair_key]

    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "vwap", "volume", "trades"])

    # Convert the str to numeric
    numeric_columns = ["open", "high", "low", "close", "vwap", "volume"]

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    # Convert from epoch ms to utc aware datetime and se as index
    df.index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df.index.name = "date"

    # Select desired column only
    df = df[["open", "high", "low", "close", "volume"]]

    return df











