"""DRAFT ingestor for tiingo (bars).

Tiingo's end-of-day endpoint returns the whole requested range in one response, so
there is no pagination: a single page, no next cursor.

**Raw prices, not adjusted.** Every record carries both — `close` beside `adjClose`,
and so on. The adjusted set is tempting and wrong for a bronze layer twice over:
mixing an adjusted close with a raw high produces a frame that fails OHLC coherence,
and adjusted values are *rewritten every time a dividend is paid*, so settled history
would change under us and trip `self_consistency` forever. Bronze is the replay log;
it stores what the venue actually traded at. `divCash` and `splitFactor` are kept so
a consumer can reconstruct the adjusted series themselves.
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests

_BASE = "https://api.tiingo.com/tiingo/daily"


class TiingoIngestor(BaseIngestor):
    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> Any:
        # Nothing to page from; return the start so the loop runs exactly once.
        return start

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        import os

        token = os.getenv("TIINGO_API_KEY", "")
        params = {"startDate": start, "token": token}
        if end:
            params["endDate"] = end

        # get_with_requests, not a bare requests.get: it applies the connect/read
        # timeout and retries transport errors. A bare call can hang forever, which
        # is the single worst failure mode for an unattended nightly.
        # get_with_requests returns the Response, not parsed JSON.
        response = get_with_requests(f"{_BASE}/{symbol}/prices", params=params)
        payload = response.json()
        return RawPage(rows=list(payload) if payload else [], next_cursor=None)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame

        # Tiingo stamps midnight UTC with an explicit Z; normalise so the index is a
        # plain UTC date like every other bars source in the lake.
        index = pd.to_datetime(frame["date"], utc=True, errors="coerce").dt.normalize()

        out = pd.DataFrame(
            {
                "open": pd.to_numeric(frame["open"], errors="coerce"),
                "high": pd.to_numeric(frame["high"], errors="coerce"),
                "low": pd.to_numeric(frame["low"], errors="coerce"),
                "close": pd.to_numeric(frame["close"], errors="coerce"),
                "volume": pd.to_numeric(frame["volume"], errors="coerce"),
            }
        )
        out.index = pd.DatetimeIndex(index, name="date")
        return out[out.index.notna()].sort_index()
