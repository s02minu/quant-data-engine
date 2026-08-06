"""CFTC Commitments of Traders (COT) positioning ingestor.

Fetches weekly **Traders in Financial Futures (TFF), futures-only** positioning
into the ``series`` group as a *multi-metric* series: for one futures market
(``series_id``), several scalars per report date — long/short contracts held by
each trader category, plus open interest. Each becomes a ``metric`` partition
(``docs/schemas/series.md``); the canonical ``(date, value)`` contract is
preserved per metric. Positioning is a Model-1 input (``docs/data-sources.md``
§3.1: "who is long/short the futures").

Source: the CFTC public reporting Socrata API (``publicreporting.cftc.gov``),
TFF futures-only dataset ``gpe5-46if``. U.S.-government public-domain data →
redistributable. No API key: the endpoint is public (an optional Socrata app
token only raises anonymous rate limits, which our weekly, ~one-market-per-call
volume never approaches).

Shape notes:
- **Weekly** (reported for Tuesday, published Friday), unlike the daily FRED/CBOE
  series. The watermark/incremental machinery is frequency-agnostic, so this
  needs no special handling — most nightly pulls simply return nothing new.
- The Socrata API filters by market and date *server-side* (a SoQL ``$where``),
  so a caught-up incremental pull returns an empty page → ``NoNewData`` (the
  benign "already up to date" case), exactly like FRED.
- ``load`` returns a **wide** frame (one column per metric); ``upsert_series_frame``
  splits it across ``metric=`` partitions. The canonical→native symbol map turns
  a friendly ticker (``ES``) into the CFTC contract market code (``13874A``).
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests

# TFF, futures-only. The Socrata resource id is stable; the story page is
# https://publicreporting.cftc.gov/stories/s/TFF-Futures-Only/98ig-3k9y/
_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
_PAGE_LIMIT = 50000  # Socrata's maximum rows per request
_DATE_COL = "report_date_as_yyyy_mm_dd"
_MARKET_COL = "cftc_contract_market_code"

# Raw CFTC column -> the metric name stored under metric=<name>. The stored value
# is the raw contract count; net positions and %-of-OI are derived features (the
# gold layer), not stored here — the platform houses the raw inputs (§2 note).
_METRICS: dict[str, str] = {
    "dealer_positions_long_all": "dealer_long",
    "dealer_positions_short_all": "dealer_short",
    "asset_mgr_positions_long": "asset_mgr_long",
    "asset_mgr_positions_short": "asset_mgr_short",
    "lev_money_positions_long": "lev_long",
    "lev_money_positions_short": "lev_short",
    "other_rept_positions_long": "other_long",
    "other_rept_positions_short": "other_short",
    "nonrept_positions_long_all": "nonrept_long",
    "nonrept_positions_short_all": "nonrept_short",
    "open_interest_all": "open_interest",
}
_SELECT = ",".join([_DATE_COL, *_METRICS])


class CftcIngestor(BaseIngestor):
    """Ingest one COT market's positioning as a multi-metric ``series``."""

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> int:
        return 0  # Socrata pages by integer offset

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        # SoQL filters by market and date server-side, so an incremental pull that
        # is already current comes back empty -> NoNewData upstream (like FRED).
        where = f"{_MARKET_COL}='{symbol}'"
        if start is not None:
            where += f" AND {_DATE_COL} >= '{start}T00:00:00'"
        if end is not None:
            where += f" AND {_DATE_COL} <= '{end}T00:00:00'"

        params: dict[str, Any] = {
            "$select": _SELECT,
            "$where": where,
            "$order": f"{_DATE_COL} ASC",
            "$limit": _PAGE_LIMIT,
            "$offset": cursor,
        }
        response = get_with_requests(_URL, params=params)  # retries; raises on 4xx/5xx
        rows = response.json()
        if not rows:
            return RawPage(rows=[], next_cursor=None)

        # A full page means there may be more; page on by offset. A single market's
        # weekly history since 2006 is ~1k rows, so this rarely runs twice.
        next_cursor = cursor + _PAGE_LIMIT if len(rows) == _PAGE_LIMIT else None
        return RawPage(rows=rows, next_cursor=next_cursor)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        raw = pd.DataFrame(rows)

        # A wide frame: a UTC date index and one column per metric. The rows are
        # already date-ascending from the query, so positional assignment is safe.
        index = pd.to_datetime(raw[_DATE_COL], utc=True)
        index.name = "date"
        out = pd.DataFrame(index=pd.DatetimeIndex(index))
        for source_col, metric in _METRICS.items():
            if source_col in raw.columns:
                # A missing value in a row coerces to NaN (kept, so the gap shows).
                out[metric] = pd.to_numeric(raw[source_col], errors="coerce").to_numpy()
            else:
                # A whole column absent from the response: an all-NaN metric column.
                out[metric] = float("nan")

        return out
