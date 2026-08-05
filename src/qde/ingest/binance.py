"""Binance OHLCV ingestor.

Time-cursor pagination against the public ``/klines`` endpoint: each page begins
one millisecond after the last kline of the previous page, and the walk ends when
a page comes back shorter than the request limit. The hand-written loader this
replaces lives in git history.
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests

# Binance klines arrive as 12-field lists; these name the fields we keep and
# which of them are numeric strings needing coercion.
_KLINE_COLUMNS = [
    "kline_open",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "kline_close",
    "quote_volume",
    "num_trades",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "unused",
]
_NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]


class BinanceIngestor(BaseIngestor):
    _URL = "https://api.binance.com/api/v3/klines"

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> int:
        # The cursor is a start time in epoch milliseconds, Binance's unit.
        return int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)

    @staticmethod
    def _end_ms(end: str | None) -> int:
        if end is None:
            return int(pd.Timestamp("now", tz="UTC").timestamp() * 1000)
        return int(pd.Timestamp(end, tz="UTC").timestamp() * 1000)

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        limit = self.spec.max_rows_per_call or 1000  # the klines endpoint's page cap
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": self._end_ms(end),
            "limit": limit,
        }

        response = get_with_requests(self._URL, params=params)  # retry helper
        if response.status_code != 200:
            raise ValueError(f"Binance API error {response.status_code}: {response.text}")

        batch = response.json()
        if not batch:
            return RawPage(rows=[], next_cursor=None)

        # A short page means the source ran out of candles: stop. Otherwise page
        # from one millisecond past the last kline's open time.
        next_cursor = None if len(batch) < limit else batch[-1][0] + 1
        return RawPage(rows=batch, next_cursor=next_cursor)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=_KLINE_COLUMNS)
        df[_NUMERIC_COLUMNS] = df[_NUMERIC_COLUMNS].apply(pd.to_numeric)

        df.index = pd.to_datetime(df["kline_open"], unit="ms", utc=True)
        df.index.name = "date"

        return df[["open", "high", "low", "close", "volume"]]
