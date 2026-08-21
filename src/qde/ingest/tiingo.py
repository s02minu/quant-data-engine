"""DRAFT ingestor for tiingo (bars).

Tiingo's end-of-day endpoint returns the whole requested range in one response, so
there is no pagination: a single page, no next cursor.

**Adjusted prices.** Every record carries both — `close` beside `adjClose`, and so on.

The first version of this stored the RAW set, reasoning that adjusted values are
rewritten whenever a dividend is paid and that bronze should hold what the venue
actually traded at. That reasoning optimised the wrong property. Raw prices make every
**split** look like a crash: XLB reads 88.47 -> 44.09 on 2025-12-05, a clean -50%
return that never happened. Measured across this source, 12 of 27 symbols carried such
artifacts — all eleven SPDR sectors from one 2:1 split, ten reverse splits in VIXY, and
a +745% print in USO. Those numbers flow straight into returns, ATR and realized vol in
the gold marts, so a "faithful" bronze row produced meaningless silver and gold.

A dividend rewrite is a nuisance the platform already handles correctly: it moves every
row by the same ratio, and `self_consistency` classifies a uniform restatement as a
corporate action (a warn that says the stored copy is an old vintage) rather than a
defect. A fake -50% return is not a nuisance — it is wrong data that nothing downstream
can detect.

Using the adjusted set also makes the lake internally consistent: the yfinance ingestor
already fetches with `auto_adjust=True`, so both equity sources now quote the same
convention.
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

        # Fail on the missing key rather than on the response it produces. Defaulting
        # to an empty token sent 27 symbols to the API and got 27 opaque 403s back —
        # a wall of "forbidden" that says nothing about the actual cause, which was
        # simply that the entry point had never loaded secrets/tiingo.env.
        token = os.getenv("TIINGO_API_KEY", "")
        if not token:
            raise ValueError(
                "TIINGO_API_KEY is not set; Tiingo rejects an empty token with 403. "
                "It is loaded from secrets/tiingo.env by qde.env.load_secrets()."
            )
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

        # The adj* set throughout — never mixed with the raw set. Taking an adjusted
        # close alongside a raw high produces a frame where `high < close`, which is
        # the exact incoherence `qde.verify` exists to catch.
        out = pd.DataFrame(
            {
                "open": pd.to_numeric(frame["adjOpen"], errors="coerce"),
                "high": pd.to_numeric(frame["adjHigh"], errors="coerce"),
                "low": pd.to_numeric(frame["adjLow"], errors="coerce"),
                "close": pd.to_numeric(frame["adjClose"], errors="coerce"),
                "volume": pd.to_numeric(frame["adjVolume"], errors="coerce"),
            }
        )
        out.index = pd.DatetimeIndex(index, name="date")
        return out[out.index.notna()].sort_index()
