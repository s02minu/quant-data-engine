import pandas as pd
import requests

from qde.loaders.http import get_with_requests


def load_binance_ohlcv(
        symbol,
        start,
        end=None,
        interval="1d",
        limit=1000
) -> pd.DataFrame:
    """ Load OHLCV data for a single symbol from Binance, returning a
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
    start_ms = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)

    # If end is None use now
    if end is None:
        end_ms = int(pd.Timestamp("now", tz="UTC").timestamp() * 1000)
    else:
        end_ms = int(pd.Timestamp(end, tz="UTC").timestamp() * 1000)


    url = "https://api.binance.com/api/v3/klines"
    all_data = []
    current_start = start_ms

    while True:
        params = {"symbol": symbol,
                  "interval": interval,
                  "startTime": current_start,
                  "endTime": end_ms,
                  "limit": limit
                  }

        response = get_with_requests(url, params=params)  # request retry helper

        # Fail test Guard for no response from binance
        if response.status_code != 200:
            raise ValueError(
                f"Binance API error {response.status_code}: {response.text}"
            )

        batch = response.json()

        # If the response comes back empty
        if not batch:
            break

        # Add batch to all_data
        all_data.extend(batch)

        # After Binance runs out of candles to send
        if len(batch) < limit:
            break

        # Advance 1 ms after the last ms retrieved
        current_start = batch[-1][0] + 1

    # Fail test Guard for no data returned but request successful
    if not all_data:
        raise ValueError(
            f"No data returned for symbol={symbol!r}, start={start!r}, end={end!r}, interval={interval!r}"
        )

    # Convert the returned data (list of lists) into a table
    df = pd.DataFrame(all_data,
                      columns=["kline_open", "open", "high", "low", "close", "volume",
                               "kline_close", "quote_volume", "num_trades",
                               "taker_buy_volume", "taker_buy_quote_volume", "unused"])

    # Convert the str to numeric
    numeric_columns = ["open", "high", "low", "close", "volume",
                       "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"]

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    # Convert from epoch ms to utc aware datetime and set as index
    df.index = pd.to_datetime(df["kline_open"], unit="ms", utc=True)
    df.index.name = "date"

    # Select desired column only
    df = df[["open", "high", "low", "close", "volume"]]

    return df



