"""Kraken OHLCV ingestor.

Cursor pagination against the public ``/0/public/OHLC`` endpoint. Kraken returns
a ``last`` cursor with every page; the walk pages forward from it and stops once
it stops advancing (the source has no rows past the cursor). Kraken caps a
response at ~720 candles, so pagination rarely runs more than once in practice.
The hand-written loader this replaces lives in git history.
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests

_OHLC_COLUMNS = ["timestamp", "open", "high", "low", "close", "vwap", "volume", "trades"]
_NUMERIC_COLUMNS = ["open", "high", "low", "close", "vwap", "volume"]

# Kraken expresses bar size in minutes.
_INTERVAL_MINUTES = {
    "2W": 21600,
    "1W": 10080,
    "1d": 1440,
    "1h": 60,
    "15m": 15,
    "5m": 5,
    "1m": 1,
}


class KrakenIngestor(BaseIngestor):
    _URL = "https://api.kraken.com/0/public/OHLC"

    @staticmethod
    def _interval_minutes(interval: str) -> int:
        minutes = _INTERVAL_MINUTES.get(interval)
        if minutes is None:
            raise ValueError(f"Unsupported interval: {interval!r}")
        return minutes

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> int:
        self._interval_minutes(interval)  # validate up front, like the source demands
        # The cursor is ``since`` in epoch seconds, Kraken's unit.
        return int(pd.Timestamp(start, tz="UTC").timestamp())

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        params = {
            "pair": symbol,
            "interval": self._interval_minutes(interval),
            "since": cursor,
        }

        response = get_with_requests(self._URL, params=params)  # retry helper
        data = response.json()

        if data["error"]:
            raise ValueError(f"Kraken API error: {data['error']}")
        if not data["result"]:
            return RawPage(rows=[], next_cursor=None)

        result = data["result"]
        pair_key = [k for k in result if k != "last"][0]
        candles = result[pair_key]
        last = result["last"]  # Kraken's cursor for the next request

        # Stop when the cursor stops advancing: a page whose ``last`` equals the
        # cursor we requested with made no progress, so the series is exhausted.
        next_cursor = None if last == cursor else last
        return RawPage(rows=candles, next_cursor=next_cursor)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=_OHLC_COLUMNS)
        df[_NUMERIC_COLUMNS] = df[_NUMERIC_COLUMNS].apply(pd.to_numeric)

        df.index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df.index.name = "date"

        # Overlapping pages can repeat a candle; last write wins.
        df = df[~df.index.duplicated(keep="last")]

        return df[["open", "high", "low", "close", "volume"]]
