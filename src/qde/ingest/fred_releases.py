"""FRED/ALFRED economic-calendar ingestor — the bitemporal ``events`` group.

Where :class:`~qde.ingest.fred.FredIngestor` fetches the *stream of values* a
figure takes (the ``series`` group), this ingestor fetches the *releases*: when a
macro figure became knowable, and every revision since (the ``events`` group, see
``docs/schemas/events.md``). A calendar that keeps only the current value silently
rewrites history — the CPI that existed at release is not the CPI after two annual
benchmark revisions — so storing *what was known, and when* is what makes the
calendar safe to backtest against (ROADMAP §3.4).

The whole revision grid comes free from ALFRED (Archival FRED): the observations
endpoint, asked for the *full* real-time range, returns one row per
``(reference period, vintage)`` — each vintage bounded by the ``realtime_start`` /
``realtime_end`` window during which that value was the reported figure:

    {realtime_start: 2024-12-11, ..., date: 2024-11-01, value: 316.441}  # initial
    {realtime_start: 2025-02-12, ..., date: 2024-11-01, value: 316.449}  # revision 1
    {realtime_start: 2026-02-13, ..., date: 2024-11-01, value: 316.528}  # revision 2

``normalize`` folds those vintages into the events schema: one **event** per
reference period (``event_id = <series>:<ref-date>``), one **row per revision**.

Mapping to the schema's two clocks (``docs/schemas/events.md``):

- **``observed_ts``** — the vintage's ``realtime_start``: when this row's value
  became known (the release, then each revision).
- **``scheduled_ts``** — the *first* vintage's ``realtime_start`` for the period:
  the release date. FRED records release dates but does not cleanly map a
  scheduled date to a not-yet-published reference period, so the first vintage —
  the day the figure first appeared — is the honest release clock. For historical
  data the scheduled and actual publication coincide (macro prints publish on
  schedule). This makes the bitemporal invariant ``observed_ts >= scheduled_ts``
  hold by construction (equality for the initial print, later for revisions).
- **``forecast``** — always ``NaN`` here: the consensus expectation is proprietary
  (Trading Economics / FMP / Bloomberg) and ships as a *code-only* enrichment
  layered on top per user (the "two halves" product shape, ROADMAP §6).

Note on ALFRED coverage: vintage history begins in the mid-1990s for most series,
so reference periods before then carry a single vintage whose ``realtime_start``
is ALFRED's coverage start, not the true 1940s release. Those events are still
well-formed; only their pre-ALFRED release timing is unknowable. Seed the calendar
from the genuine-revision era (the platform seeds from 2000) to keep it meaningful.

The API key is read from ``FRED_API_KEY`` (shared with the series ingestor via
``fred.fred_api_key``), the same secrets-via-env pattern as the R2 credentials.
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.ingest.fred import fred_api_key
from qde.loaders.http import get_with_requests

_URL = "https://api.stlouisfed.org/fred/series/observations"
_PAGE_LIMIT = 100000  # FRED's maximum observations per request

# The full real-time range: asking for every vintage ever recorded. FRED's own
# sentinels — the earliest representable real-time date and its open-ended future.
_REALTIME_START = "1776-07-04"
_REALTIME_END = "9999-12-31"


class FredReleasesIngestor(BaseIngestor):
    """Ingest a FRED series' full vintage history as bitemporal ``events`` rows."""

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> int:
        return 0  # the observations endpoint pages by integer offset

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        params: dict[str, Any] = {
            "series_id": symbol,
            "api_key": fred_api_key(),
            "file_type": "json",
            "offset": cursor,
            "limit": _PAGE_LIMIT,
            "sort_order": "asc",
            # Ask for every vintage, not just the current value — this is what turns
            # a plain observations pull into the full revision history.
            "realtime_start": _REALTIME_START,
            "realtime_end": _REALTIME_END,
        }
        # start/end bound the *reference period* (which figures), not the vintage.
        if start is not None:
            params["observation_start"] = start
        if end is not None:
            params["observation_end"] = end

        response = get_with_requests(_URL, params=params)  # retry helper; raises on 4xx/5xx
        observations = response.json().get("observations", [])
        if not observations:
            return RawPage(rows=[], next_cursor=None)

        # Carry the series id on each row so normalize can build event_id without
        # threading mutable state through the shared pagination loop (fetch_page
        # receives the native symbol; the fredcal map is identity, so it is the
        # FRED series id).
        for obs in observations:
            obs["series_id"] = symbol

        next_cursor = cursor + _PAGE_LIMIT if len(observations) == _PAGE_LIMIT else None
        return RawPage(rows=observations, next_cursor=next_cursor)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        """Fold ``(reference period, vintage)`` rows into the events schema.

        One event per reference period, one row per revision. See
        ``docs/schemas/events.md`` for the column contract.
        """
        raw = pd.DataFrame(rows, columns=["series_id", "date", "realtime_start", "value"])

        # A missing observation (FRED's ".") is coerced to NaN and the row kept, so
        # an unpublished/withheld print stays visible rather than vanishing.
        actual = pd.to_numeric(raw["value"], errors="coerce")
        observed_ts = pd.to_datetime(raw["realtime_start"], utc=True)
        ref_date = pd.to_datetime(raw["date"], utc=True)

        df = pd.DataFrame(
            {
                "series_id": raw["series_id"].astype(str),
                "ref_date": ref_date,
                "observed_ts": observed_ts,
                "actual": actual,
            }
        )
        # Order vintages within each reference period so revision_seq counts up from
        # the initial print (the earliest realtime_start).
        df = df.sort_values(["series_id", "ref_date", "observed_ts"]).reset_index(drop=True)

        grp = df.groupby(["series_id", "ref_date"], sort=False)
        df["revision_seq"] = grp.cumcount()
        # scheduled_ts is constant per event: the first vintage's date (the release).
        df["scheduled_ts"] = grp["observed_ts"].transform("min")

        df["event_id"] = df["series_id"] + ":" + df["ref_date"].dt.strftime("%Y-%m-%d")

        # previous = the prior reference period's initial print, the figure a release
        # announcement shows as "previous". A documented approximation of "the prior
        # period's value at release time" — exact bitemporal previous (the P-1 value
        # as known at *this* row's observed_ts) is derivable but adds little for a
        # column no DQ check depends on. NaN for the earliest period of each series.
        initial = (
            df[df["revision_seq"] == 0]
            .set_index(["series_id", "ref_date"])["actual"]
            .sort_index()
        )
        prev = initial.groupby(level="series_id").shift(1)
        # .to_dict() (a Mapping keyed by the (series_id, ref_date) tuple) rather than
        # passing the Series directly, which Index.map's type does not accept.
        df["previous"] = df.set_index(["series_id", "ref_date"]).index.map(prev.to_dict())

        # forecast (consensus) is the proprietary, code-only column — never free.
        df["forecast"] = pd.Series([float("nan")] * len(df), dtype="float64")

        return df[
            [
                "event_id",
                "series_id",
                "scheduled_ts",
                "observed_ts",
                "actual",
                "forecast",
                "previous",
                "revision_seq",
            ]
        ].reset_index(drop=True)
