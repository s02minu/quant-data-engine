"""FRED (Federal Reserve Economic Data) series ingestor.

Fetches observations for a FRED series into the canonical ``series`` shape — a
UTC ``date`` index and a single ``value`` column (see ``docs/schemas/series.md``).
FRED is the macro spine of the platform (``docs/data-sources.md``): free, and the
curated U.S.-government series in the registry are public-domain / redistributable.

The API key is read from the ``FRED_API_KEY`` environment variable, the same
secrets-via-env pattern as the R2 credentials — never committed to the repo.
"""

import os
from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests

_URL = "https://api.stlouisfed.org/fred/series/observations"
_PAGE_LIMIT = 100000  # FRED's maximum observations per request


def fred_api_key() -> str:
    """Return the FRED API key from the environment, or raise if it is unset.

    Shared by every FRED-backed ingestor (the ``series`` observations here and
    the ``events`` release calendar in ``fred_releases``) so the key guard and
    its message live in one place.
    """
    key = os.getenv("FRED_API_KEY")
    if not key:
        raise RuntimeError(
            "FRED_API_KEY is not set. Get a free key at https://fred.stlouisfed.org "
            "and export it (e.g. a gitignored secrets/fred.env)."
        )
    return key


class FredIngestor(BaseIngestor):
    """Ingest a FRED series' observations as a scalar ``series``."""

    @staticmethod
    def _api_key() -> str:
        return fred_api_key()

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> int:
        return 0  # the observations endpoint pages by integer offset

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        params: dict[str, Any] = {
            "series_id": symbol,
            "api_key": self._api_key(),
            "file_type": "json",
            "offset": cursor,
            "limit": _PAGE_LIMIT,
            "sort_order": "asc",
        }
        if start is not None:
            params["observation_start"] = start
        if end is not None:
            params["observation_end"] = end

        response = get_with_requests(_URL, params=params)  # retry helper; raises on 4xx/5xx
        observations = response.json().get("observations", [])
        if not observations:
            return RawPage(rows=[], next_cursor=None)

        # A full page means there may be more, so page on by offset. FRED series
        # are small (daily since the 1940s is well under the 100k cap), so this
        # rarely runs more than once.
        next_cursor = cursor + _PAGE_LIMIT if len(observations) == _PAGE_LIMIT else None
        return RawPage(rows=observations, next_cursor=next_cursor)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=["date", "value"])

        # FRED sends a missing observation as ".": coerce to NaN and keep the row
        # so the gap stays visible rather than being silently dropped.
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.index = pd.to_datetime(df["date"], utc=True)
        df.index.name = "date"

        return df[["value"]]
