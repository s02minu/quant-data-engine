"""ccxt-backed OHLCV ingestor — one implementation, many exchanges.

The clean way to widen crypto-venue coverage (``docs/data-sources.md`` §3.3, Wave
2): instead of a bespoke loader per exchange, one :class:`BaseIngestor` subclass
drives ccxt's unified ``fetch_ohlcv`` against whichever venue a registry row names.
Each exchange is a ``bars`` ``SourceSpec`` whose **name is the ccxt exchange id**
(``coinbase``, ``bybit``, ``okx``, ``kucoin``, …); the dispatch points them all at
this class, and the spec's symbol map turns a canonical symbol into that venue's
ccxt symbol (``BTCUSDT`` → ``BTC/USDT``, or Coinbase's ``BTC/USD``). Exchange-native
public market data, so redistributable — verify each venue's API terms before
publishing.

Pagination walks forward by time: each page starts one millisecond after the last
candle returned, and it ends on an empty page — *not* on a page shorter than the
requested limit, because ccxt silently clamps ``limit`` to each exchange's own cap,
so a short page is normal mid-history. An empty page is disambiguated by whether
data has already started: **after** the first candle it means the present is reached
(stop → the base raises :class:`NoNewData` if nothing came back at all, the benign
"already up to date" signal); **before** it, the window predates the pair's listing —
some venues return ``[]`` for a pre-listing window rather than clamping to the
earliest candle — so the walk probes forward until it finds the listing. The probe
step stays under any venue's page span, so an empty window is never skipped over data.
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage

_OHLCV_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]
# Forward step across an empty pre-listing window. Must stay under the smallest
# venue page span (observed ~300 daily candles) so probing never skips over data.
_EMPTY_PROBE_MS = 90 * 86_400_000  # 90 days


def _build_exchange(exchange_id: str) -> Any:
    """Construct a rate-limited ccxt exchange client by id.

    A module-level seam so tests can inject a fake exchange without importing
    ccxt or touching the network. ``enableRateLimit`` lets ccxt pace requests to
    each venue's published limit, so a full-history backfill does not get banned.
    """
    import ccxt

    return getattr(ccxt, exchange_id)({"enableRateLimit": True})


class CcxtIngestor(BaseIngestor):
    """Ingest OHLCV bars from any ccxt exchange named by its ``SourceSpec``."""

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._exchange: Any | None = None
        self._seen_data = False

    def _ex(self) -> Any:
        # Built once per ingestor and reused across pages (loading markets is not
        # free); get_ingestor makes a fresh ingestor per load, so no stale state.
        if self._exchange is None:
            self._exchange = _build_exchange(self.spec.name)
        return self._exchange

    @staticmethod
    def _ms(when: str) -> int:
        return int(pd.Timestamp(when, tz="UTC").timestamp() * 1000)

    @staticmethod
    def _now_ms() -> int:
        return int(pd.Timestamp("now", tz="UTC").timestamp() * 1000)

    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> int:
        self._seen_data = False  # reset per load so a reused ingestor stays correct
        return self._ms(start)  # ccxt `since` is epoch milliseconds

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        limit = self.spec.max_rows_per_call  # a hint; ccxt clamps to the venue's max
        batch = self._ex().fetch_ohlcv(symbol, timeframe=interval, since=cursor, limit=limit)
        upper_ms = self._ms(end) if end is not None else self._now_ms()

        if not batch:
            # After data started, an empty window means we've caught up -> stop.
            # Before it, the window predates the pair's listing (some venues return
            # [] rather than the earliest candle), so probe forward until the range
            # end; the base then raises NoNewData only if nothing was ever found.
            if self._seen_data:
                return RawPage(rows=[], next_cursor=None)
            probe = cursor + _EMPTY_PROBE_MS
            return RawPage(rows=[], next_cursor=None if probe >= upper_ms else probe)

        # No end param on fetch_ohlcv: filter client-side; empty after filtering
        # means we've passed the requested end, so stop.
        if end is not None:
            batch = [row for row in batch if row[0] <= upper_ms]
            if not batch:
                return RawPage(rows=[], next_cursor=None)

        # Walk forward from just after the last candle. The guard stops a venue that
        # fails to advance so the loop can never spin.
        self._seen_data = True
        next_cursor = batch[-1][0] + 1
        if next_cursor <= cursor:
            next_cursor = None
        return RawPage(rows=batch, next_cursor=next_cursor)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=_OHLCV_COLUMNS)
        # Overlapping pages can repeat a boundary candle; dedupe by timestamp.
        df = df.drop_duplicates(subset="ts")
        df[_OHLCV_COLUMNS[1:]] = df[_OHLCV_COLUMNS[1:]].apply(pd.to_numeric)

        df.index = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df.index.name = "date"
        return df[["open", "high", "low", "close", "volume"]]
