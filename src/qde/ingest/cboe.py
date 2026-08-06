"""CBOE volatility-complex series ingestor.

Fetches end-of-day levels for CBOE's volatility indices — VIX, VVIX, SKEW — into
the canonical ``series`` shape (a UTC ``date`` index and a single ``value``
column; see ``docs/schemas/series.md``). These are Model-1 volatility inputs
(``docs/data-sources.md`` §3.1); EOD index levels are published freely and are
treated as redistributable (see the spec's ``license_note``).

CBOE publishes each index's *entire* daily history as one CSV on its CDN, e.g.
``https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv``. So,
unlike FRED's paginated API, there is no date-range parameter: the ingestor
downloads the whole file and narrows to ``[start, end]`` client-side. When an
incremental pull is already caught up the slice is empty, which the base turns
into ``NoNewData`` — the benign "already up to date" case, exactly mirroring
FRED's empty page. No API key: the CSVs are public.

The three files differ in shape — VIX carries OPEN/HIGH/LOW/CLOSE while VVIX and
SKEW carry a single value column — but share a uniform rule: **DATE is the first
column, the EOD level is the last** (``CLOSE`` for VIX, the lone value column for
the others). That one rule handles all three without per-series configuration.
"""

import io
from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests

_BASE = "https://cdn.cboe.com/api/global/us_indices/daily_prices"
_DATE_FMT = "%m/%d/%Y"  # CBOE dates are MM/DD/YYYY


class CboeIngestor(BaseIngestor):
    """Ingest a CBOE volatility index' EOD levels as a scalar ``series``."""

    @staticmethod
    def _url(symbol: str) -> str:
        # Each index is served at ``{SYMBOL}_History.csv`` on the CDN.
        return f"{_BASE}/{symbol}_History.csv"

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> str:
        # No cursor to page from; return the start so the loop runs exactly once.
        return start

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        response = get_with_requests(self._url(symbol), params=None)  # retries; raises on 4xx/5xx
        df = pd.read_csv(io.StringIO(response.text))

        # Parse DATE (the first column) to a UTC index up front, because the
        # range filter below needs it — the CDN serves the whole history with no
        # date parameter, so we narrow to [start, end] here rather than server-side.
        df.index = pd.to_datetime(df.iloc[:, 0], format=_DATE_FMT, utc=True)
        df.index.name = "date"

        if start is not None:
            df = df[df.index >= pd.Timestamp(start, tz="UTC")]
        if end is not None:
            df = df[df.index <= pd.Timestamp(end, tz="UTC")]

        # An empty slice means nothing newer in range; yield nothing so the base
        # raises NoNewData (already up to date), just like FRED's empty page.
        if df.empty:
            return RawPage(rows=[], next_cursor=None)

        return RawPage(rows=[df], next_cursor=None)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        df = rows[0]

        # The EOD level is the last column: CLOSE for VIX (which also carries
        # OPEN/HIGH/LOW), or the single VVIX/SKEW column. Coerce defensively; a
        # stray non-numeric becomes NaN with the row kept, so a gap stays visible.
        value = pd.to_numeric(df.iloc[:, -1], errors="coerce")
        out = value.to_frame(name="value")
        out.index.name = "date"
        return out
