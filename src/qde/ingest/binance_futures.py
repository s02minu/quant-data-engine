"""Binance USD-M perpetual funding-rate ingestor.

Fetches historical funding for a Binance USD-margined perpetual into the ``series``
group as a *multi-metric* series: per settlement (every 8h) the **funding rate**
and the **mark price** at settlement. Funding / open interest / liquidations are
the Model-2 order-flow confluences (``docs/data-sources.md`` §3.1); funding is the
one with clean, complete public history and so is what lands here.

- **Open interest** has only ~30 days of REST history (``/futures/data/openInterestHist``)
  — no deep backfill, so it is a forward-snapshot job for later, not this ingestor.
- **Liquidations** have no public historical REST (the ``allForceOrders`` endpoint
  is gone / 404); they are a streaming concern (the ``!forceOrder`` websocket).

Exchange-native public REST (``fapi.binance.com``) → redistributable; no API key.
This is the perp/derivatives feed — a source *distinct* from the spot ``binance``
bars (one ``SourceSpec`` is one group), reusing the same canonical crypto symbols.

Time-cursor pagination mirrors the spot klines ingestor: each page begins one
millisecond after the last settlement, and the walk ends on a short page. The
endpoint filters by ``startTime`` server-side, so a caught-up incremental pull
returns an empty page → ``NoNewData`` (the benign "already up to date" case).
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests


class BinanceFuturesIngestor(BaseIngestor):
    """Ingest a Binance USD-M perp's funding history as a multi-metric ``series``."""

    _URL = "https://fapi.binance.com/fapi/v1/fundingRate"

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> int:
        # The cursor is a start time in epoch milliseconds, Binance's unit.
        return int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)

    @staticmethod
    def _end_ms(end: str | None) -> int:
        ts = pd.Timestamp("now", tz="UTC") if end is None else pd.Timestamp(end, tz="UTC")
        return int(ts.timestamp() * 1000)

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        limit = self.spec.max_rows_per_call or 1000  # the fundingRate endpoint's page cap
        params = {
            "symbol": symbol,
            "startTime": cursor,
            "endTime": self._end_ms(end),
            "limit": limit,
        }

        response = get_with_requests(self._URL, params=params)  # retry helper
        if response.status_code != 200:
            raise ValueError(f"Binance futures API error {response.status_code}: {response.text}")

        batch = response.json()
        if not batch:
            return RawPage(rows=[], next_cursor=None)

        # A short page means the source ran out of settlements: stop. Otherwise page
        # from one millisecond past the last settlement's time.
        next_cursor = None if len(batch) < limit else batch[-1]["fundingTime"] + 1
        return RawPage(rows=batch, next_cursor=next_cursor)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        df = pd.DataFrame(rows)

        # A wide frame: a UTC settlement-time index and one column per metric, which
        # upsert_series_frame splits into metric=funding_rate / metric=mark_price.
        index = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
        index.name = "date"
        out = pd.DataFrame(index=pd.DatetimeIndex(index))
        # The earliest settlements carry an empty markPrice -> NaN (row kept).
        out["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce").to_numpy()
        out["mark_price"] = pd.to_numeric(df["markPrice"], errors="coerce").to_numpy()

        return out
