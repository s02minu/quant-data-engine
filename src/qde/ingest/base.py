"""``BaseIngestor`` — the shared ingestion machinery for the little book.

Everything that is *constant across sources* lives here once (ROADMAP §3.2):

- **symbol translation** — canonical → source-native, via the :class:`SourceSpec`;
- **the pagination loop** — walk pages from a cursor until the source signals the
  end, accumulating raw records;
- **the empty-result contract** — a successful pull that yields nothing raises
  :class:`NoNewData`, so the benign "already up to date" case is one exception in
  one place rather than duplicated per loader.

HTTP retry/backoff also lives once, in :func:`qde.loaders.http.get_with_requests`,
which the concrete fetchers call.

A concrete source implements only the parts that genuinely differ between APIs:

- ``first_cursor`` — where a pull starts (a start time, an epoch cursor, …);
- ``fetch_page`` — fetch one page of raw records and say where the next begins;
- ``normalize`` — shape the accumulated raw records into the canonical bars frame
  (a UTC ``date`` index and ``open/high/low/close/volume`` columns).

Adding a source is then those three small methods plus a registry row.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qde.loaders.exceptions import NoNewData
from qde.registry.spec import SourceSpec


@dataclass
class RawPage:
    """One page of raw records plus the cursor for the page after it.

    Attributes:
        rows: The raw records this page yielded, in the source's own shape —
            Binance and Kraken return lists of fields; yfinance returns a
            one-element list holding its DataFrame. The loop concatenates ``rows``
            across pages and hands the flat list to ``normalize``.
        next_cursor: The cursor that fetches the following page, or ``None`` when
            this was the last page. A single-shot source returns ``None`` after
            its only page.
    """

    rows: list[Any]
    next_cursor: Any | None


class BaseIngestor(ABC):
    """Base class for a bar-series ingestor bound to one :class:`SourceSpec`."""

    def __init__(self, spec: SourceSpec) -> None:
        self.spec = spec

    def load(
        self, symbol: str, start: str, end: str | None = None, interval: str = "1d"
    ) -> pd.DataFrame:
        """Load a bar series for a *canonical* symbol.

        The symbol is translated to the source's native spelling via the spec,
        then handed to :meth:`load_native`.
        """
        native = self.spec.native(symbol)
        return self.load_native(native, start, end, interval)

    def load_native(
        self, symbol: str, start: str, end: str | None = None, interval: str = "1d"
    ) -> pd.DataFrame:
        """Load a bar series for a source-*native* symbol.

        Runs the shared pagination loop, then normalizes. A pull that returns no
        rows raises :class:`NoNewData` — the benign "nothing newer to fetch"
        case, distinct from a real failure (an unknown symbol or API error),
        which the concrete fetcher raises as a plain ``ValueError``.

        Raises:
            NoNewData: the source had no rows in range.
        """
        rows: list[Any] = []
        cursor = self.first_cursor(symbol, start, end, interval)
        while cursor is not None:
            page = self.fetch_page(symbol, cursor, start, end, interval)
            rows.extend(page.rows)
            cursor = page.next_cursor

        if not rows:
            raise NoNewData(
                f"No data returned for symbol={symbol!r}, start={start!r}, "
                f"end={end!r}, interval={interval!r} from {self.spec.name}"
            )

        return self.normalize(rows)

    @abstractmethod
    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> Any:
        """Return the cursor the first page is fetched from."""

    @abstractmethod
    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        """Fetch one page of raw records and the cursor for the next one."""

    @abstractmethod
    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        """Shape accumulated raw records into the canonical bars frame."""
