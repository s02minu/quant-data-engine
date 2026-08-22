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

import time
from abc import ABC, abstractmethod
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qde.loaders import budget
from qde.loaders.exceptions import NoNewData
from qde.registry.spec import SourceSpec

# A ceiling on one series' walk. Deliberately far above any legitimate pull — a
# full daily-bar history at the smallest page size any venue uses is a few hundred
# pages — so reaching it means the walk is not converging, not that the range was
# ambitious. Paired with the repeated-cursor check below: that catches a cursor
# stuck in place, this catches one that advances but never arrives.
MAX_PAGES = 10_000


# When each source was last called, so the declared rate limit is honoured across a
# whole run rather than per ingestor instance — `get_ingestor` builds a fresh object
# per symbol, so per-instance state would pace nothing.
_LAST_CALL: dict[str, float] = {}


def _throttle(source: str, per_minute: int | None) -> None:
    """Wait, if needed, so ``source`` is not called faster than it permits.

    ``rate_limit_per_min`` sat in the registry as documentation for months while
    nothing enforced it — a field that reads like a control and behaves like a
    comment. It became load-bearing with Tiingo, whose free tier caps requests per
    hour: one nightly pass over 27 symbols fits, and a manual re-run in the same hour
    does not. Without pacing the only defence is the retry/backoff in
    ``qde.loaders.http``, which turns a predictable wait into four failed attempts
    and then an exception.

    A source declaring no limit is not delayed at all, so this costs nothing for the
    exchanges that page freely.
    """
    if not per_minute or per_minute <= 0:
        return
    interval = 60.0 / per_minute
    last = _LAST_CALL.get(source)
    now = time.monotonic()
    if last is not None:
        wait = interval - (now - last)
        if wait > 0:
            time.sleep(wait)
    _LAST_CALL[source] = time.monotonic()


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
        seen_cursors: set[Any] = set()
        pages = 0

        while cursor is not None:
            # A cursor that does not move means the source is handing back the same
            # page forever. Unguarded, that is an infinite loop against a remote API
            # with an ever-growing list in memory — and the failure mode is a nightly
            # that never finishes rather than one that fails, so nothing alerts and
            # the process looks alive the entire time. Better to stop and say why.
            key = cursor if isinstance(cursor, Hashable) else repr(cursor)
            if key in seen_cursors:
                raise ValueError(
                    f"{self.spec.name} pagination stalled at cursor {cursor!r} for "
                    f"symbol={symbol!r}: the source returned a cursor it had already "
                    "issued, so the walk would never terminate"
                )
            seen_cursors.add(key)

            pages += 1
            if pages > MAX_PAGES:
                raise ValueError(
                    f"{self.spec.name} exceeded {MAX_PAGES} pages for symbol={symbol!r} "
                    f"(start={start!r}, end={end!r}) — refusing to keep walking. Widen "
                    "the page size or narrow the range."
                )

            _throttle(self.spec.name, self.spec.rate_limit_per_min)
            # The hourly budget is charged inside get_with_requests rather than here,
            # so retries are counted too; this only says whose budget it is. Set at
            # the base rather than in each fetch_page so a source added tomorrow is
            # metered without anyone remembering to wire it.
            with budget.fetching(self.spec.name):
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
