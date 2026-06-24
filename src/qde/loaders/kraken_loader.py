import pandas as pd
import requests

from qde.loaders.http import get_with_requests


def load_kraken_ohlcv(symbol, start, end=None, interval="1d") -> pd.DataFrame:
    """ Load OHLCV data for a single symbol from Kraken, returning a
        cleaned DataFrame with flat lowercase columns and a UTC-aware index.

        Args:
            symbol (str): a ticker symbol.
            start (str): the time period to begin.
            end (str): the time period to end.
            interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'.


        Returns:
            DataFrame with columns: date, open, high, low, close, volume.
            Index by a UTC-aware DatetimeIndex named 'date'.

        Raises:
            ValueError: If the API returns a non-200 status or an empty response.
    """
    # Get the request
    url = "https://api.kraken.com/0/public/OHLC"

    # Kraken Interval mapping
    interval_map = {
        "2W": 21600,
        "1W": 10080,
        "1d": 1440,
        "1h": 60,
        "15m": 15,
        "5m": 5,
        "1m": 1,
    }

    kraken_interval = interval_map.get(interval)
    if kraken_interval is None:
        raise ValueError(f"Unsupported interval: {interval!r}")

    # Convert start to epoch ms
    since = int(pd.Timestamp(start, tz="UTC").timestamp())

    # Take care of pagination

    all_candles = []
    prev_since = None           # tracks the previous cursor, to detect "no progress"

    while True:
        params = {
            "pair": symbol,
            "interval": kraken_interval,
            "since": since,
        }

        response = get_with_requests(url, params=params)  # request retry helper
        data = response.json()

        # Guard against error
        if data["error"]:
            raise ValueError(f"Kraken API error: {data['error']}")

        # Guard against empty result
        if not data["result"]:
            raise ValueError()

        result = data["result"]
        pair_key = [k for k in result if k != "last"][0]
        candles = result[pair_key]

        all_candles.extend(candles)

        last = result["last"] # Kraken's cursor for the next request

        if last == prev_since:
            break

        prev_since = since
        since = last

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "vwap", "volume", "trades"])

    # Convert the str to numeric
    numeric_columns = ["open", "high", "low", "close", "vwap", "volume"]

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    # Convert from epoch ms to utc aware datetime and se as index
    df.index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df.index.name = "date"

    # Prevent duplicating
    df = df[~df.index.duplicated(keep="last")]

    # Select desired column only
    df = df[["open", "high", "low", "close", "volume"]]

    return df











