"""Venue-neutral websocket capture loop.

The exchange-specific dialect — stream naming, message decoding, and how the
order book is anchored — lives behind a `VenueAdapter` (see `qde.stream.venues`).
This loop owns only what every venue shares: buffering, timed flush, reconnect
with backoff, sequence-gap tracking, session markers, and the bronze layout.
"""

import asyncio
import json
from collections import defaultdict
from datetime import UTC, datetime

import pandas as pd
import websockets
from websockets.exceptions import WebSocketException

from qde.log import configure, get_logger
from qde.stream.config import StreamConfig
from qde.stream.gaps import (
    SESSION_START,
    SESSION_STOP,
    SequenceTracker,
    reconnect_gap,
    session_record,
)
from qde.stream.parsers import now_ms
from qde.stream.paths import bronze_path
from qde.stream.venues import get_adapter

log = get_logger(__name__)

# Session markers describe the whole collector, not one symbol, so they use a
# sentinel in the symbol partition. The leading underscore keeps it from ever
# colliding with a real ticker.
SESSION_SYMBOL = "_all"


class StreamCollector:
    """Captures an exchange's streams to the bronze layer.

    Connects via the configured venue adapter, decodes each message through it,
    buffers rows by (kind, symbol), and flushes micro-batches to partitioned
    Parquet. Survives disconnects with backoff, records sequence gaps and
    session boundaries, and — for venues that need it — periodically anchors the
    diff-depth stream with REST snapshots.
    """

    def __init__(self, config: StreamConfig):
        self.config = config
        # The venue adapter holds everything exchange-specific; the loop below
        # is dialect-free. `config.source` selects it (default "binance").
        self.adapter = get_adapter(config.source)
        self.count = 0
        # Rows wait here between flushes. Keyed by (kind, symbol) because that
        # pair identifies one bronze partition, so a buffer maps to one file.
        self.buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self.sequences = SequenceTracker()
        self.gap_count = 0
        self._disconnected_at: int | None = None

    async def run(self, max_messages: int | None = None) -> None:
        """Open the connection and read messages until stopped.

        Args:
            max_messages: Stop after this many messages. Use None to run
                indefinitely (the real capture mode); a small number is handy
                for a bounded demo.
        """
        url = self.adapter.ws_url(self.config)

        # Marks the boundary a restart would otherwise leave silent: any data
        # before it belongs to a prior session, and the span since that session
        # ended is downtime.
        self._record_session(SESSION_START, now_ms())

        # The second concurrent task: it sleeps between flushes while the read
        # loop below keeps draining the socket. An unread socket would back up
        # and eventually be dropped by the exchange.
        flush_task = asyncio.create_task(self._flush_loop())
        snapshot_task = asyncio.create_task(self._snapshot_loop())

        backoff = 1

        try:
            # Outer loop: a dropped connection is expected, not exceptional.
            while True:
                try:
                    # opens the connection and guarantees the connection is cleanly closed.
                    # max_size follows the venue: Coinbase's inline book snapshot is
                    # >1 MiB and would trip the websockets default frame limit.
                    async with websockets.connect(url, max_size=self.adapter.max_frame_bytes) as ws:
                        # Some venues (Coinbase) subscribe with a frame after
                        # connecting; Binance encodes it in the URL, so this is empty.
                        for frame in self.adapter.subscribe_frames(self.config):
                            await ws.send(json.dumps(frame))

                        self._on_connected()
                        backoff = 1  # reset only after a connection succeeds

                        # Anchor this connection before consuming its deltas.
                        # The socket is already open and buffering, so no diffs
                        # are lost while the snapshot is fetched; any that
                        # predate last_update_id are discarded on replay. A no-op
                        # for venues that deliver the snapshot inline.
                        await self._snapshot_all()

                        connected_at = now_ms()

                        # Yields one message per push and pauses in between without
                        # blocking the process; runs until the socket closes.
                        async for raw in ws:
                            # Stamped before any parsing work so the value reflects
                            # arrival, not how long processing happened to take.
                            received_at = now_ms()
                            self._handle(json.loads(raw), received_at)
                            if max_messages is not None and self.count >= max_messages:
                                return
                            if self._should_recycle(connected_at, received_at):
                                self._begin_recycle(received_at)
                                break  # closes the socket; the outer loop reconnects

                except (WebSocketException, OSError) as exc:
                    # Buffered rows are written before waiting, so an outage never
                    # sits on top of unsaved data.
                    self._disconnected_at = now_ms()
                    self.flush()
                    log.warning("connection_lost", error=type(exc).__name__, retry_in_s=backoff)
                    await asyncio.sleep(backoff)
                    # Exponential backoff, capped: a long outage must not turn into
                    # a tight reconnect loop against the exchange.
                    backoff = min(backoff * 2, 60)
        finally:
            # Runs on normal exit, on error, and on Ctrl-C. Without this final
            # flush everything buffered since the last interval would be lost,
            # and streamed data cannot be re-fetched. A best-effort stop marker
            # closes the session; a hard kill (SIGKILL) skips it, in which case
            # the next start marker still bounds the downtime.
            flush_task.cancel()
            snapshot_task.cancel()
            self._record_session(SESSION_STOP, now_ms(), self.count, self.gap_count)
            self.flush()

    def _should_recycle(self, connected_at: int, now: int) -> bool:
        """Has this connection been open long enough to be worth replacing?

        A connection that never closes is not the safe state it looks like. Binance
        drops a burst of messages on a long-lived one **without closing it**: measured
        on this lake, every 48.2-48.9h of continuous connection, all six streams jumped
        their sequence numbers within 2ms of each other and carried on — 2,931 depth
        messages gone in the worst case, no disconnect, no error, nothing to catch. It
        went unnoticed for weeks because frequent deploys kept restarting the collector
        and no connection ever survived long enough to reach it.

        Cycling on our own schedule turns that into a reconnect we control: the outer
        loop's existing path flushes, records a ``reconnect`` gap (benign, ~2s wide,
        already tolerated by the DQ checks) and re-anchors depth from a fresh snapshot.
        A known 2-second hole beats a silent 3,000-message one.

        Reuses the caller's receive timestamp rather than reading the clock again —
        this runs once per message, and the message already carries the time.
        """
        limit = self.config.max_connection_seconds
        return limit > 0 and (now - connected_at) >= limit * 1000

    def _begin_recycle(self, at_ms: int) -> None:
        """Mark a deliberate reconnect so it is recorded like any other outage.

        Sets the same field an unexpected drop sets, so ``_on_connected`` writes the
        reconnect gap records and resets sequence state through one code path. The
        cycle is deliberate; the hole it leaves is real, and it gets recorded either
        way — the lake should not claim continuity it does not have.
        """
        log.info(
            "connection_recycle",
            after_s=self.config.max_connection_seconds,
            reason="pre-empt long-connection message loss",
        )
        self._disconnected_at = at_ms
        self.flush()

    def _handle(self, message: dict, received_at: int) -> None:
        """Decode one message, buffer it, and check sequence continuity.

        A message the adapter does not capture (a subscription acknowledgement,
        say) decodes to None and is counted but not buffered.
        """
        self.count += 1
        routed = self.adapter.route(message, received_at)
        if routed is None:
            return
        kind, row = routed
        symbol = row["symbol"]
        self.buffers[(kind, symbol)].append(row)

        gap = self.sequences.check(kind, symbol, row)
        if gap is not None:
            self._record_gap(gap)

    def _record_gap(self, gap: dict) -> None:
        """Buffer a gap record and announce it.

        Gaps are stored under their own partition rather than as a column on
        the affected rows, so a hole is queryable without scanning the tape.
        """
        self.gap_count += 1
        self.buffers[("gaps", gap["symbol"])].append(gap)
        log.warning(
            "gap",
            kind=gap["stream_kind"],
            symbol=gap["symbol"],
            reason=gap["reason"],
            missing=gap["missing_count"],
        )

    def _record_session(
        self, event: str, at_ms: int, message_count: int | None = None, gap_count: int | None = None
    ) -> None:
        """Buffer a session start/stop marker under the session partition."""
        record = session_record(event, at_ms, message_count, gap_count)
        self.buffers[("session", SESSION_SYMBOL)].append(record)
        # 'event' is structlog's reserved first-positional name, so use 'state'.
        log.info("session", state=event, at_ms=at_ms)

    def _on_connected(self) -> None:
        """Record the outage that preceded this connection, if any."""
        if self._disconnected_at is None:
            return

        reconnected_at = now_ms()
        for kind, symbol in self.sequences.reset():
            self._record_gap(reconnect_gap(kind, symbol, self._disconnected_at, reconnected_at))
        self._disconnected_at = None

    def flush(self) -> int:
        """Write every non-empty buffer to a Parquet part file.

        Each (kind, symbol) buffer becomes one file under its bronze partition.
        Buffers are detached before writing so the read loop can keep appending
        to a fresh list while the previous batch is on its way to disk.

        Returns:
            Number of rows written.
        """
        batch_time = datetime.now(UTC)
        written = 0

        for key in list(self.buffers):
            rows = self.buffers[key]
            if not rows:
                continue
            self.buffers[key] = []

            kind, symbol = key
            path = bronze_path(self.config, kind, symbol, batch_time)
            path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_parquet(path, engine="pyarrow", index=False)

            written += len(rows)
            log.info("flushed", rows=len(rows), kind=kind, symbol=symbol, path=str(path))

        return written

    async def _flush_loop(self) -> None:
        """Flush on a fixed interval, concurrently with the read loop."""
        while True:
            await asyncio.sleep(self.config.flush_seconds)
            self.flush()

    async def _snapshot_all(self) -> None:
        """Buffer a REST order-book snapshot for every configured symbol.

        Only for venues whose diff stream needs a separately fetched anchor
        (Binance); venues that deliver the snapshot inline over the socket
        (Coinbase) return `rest_snapshots=False` and skip this entirely. A
        failed snapshot is logged and skipped: it costs an anchor point, but it
        must never take down a capture that is still receiving live data.
        """
        if not self.adapter.rest_snapshots or "depth" not in self.config.kinds:
            return

        for symbol in self.config.symbols:
            try:
                # to_thread keeps the blocking HTTP call off the event loop, so
                # the socket keeps draining while the request is in flight.
                row = await asyncio.to_thread(
                    self.adapter.fetch_snapshot, self.config, symbol, now_ms()
                )
            except Exception as exc:
                log.warning(
                    "snapshot_failed", symbol=symbol, error=type(exc).__name__, detail=str(exc)
                )
                continue

            self.buffers[("snapshot", symbol)].append(row)
            log.info("snapshot", symbol=symbol, last_update_id=row["last_update_id"])

    async def _snapshot_loop(self) -> None:
        """Take snapshots on a fixed interval.

        Sleeps first: the anchor for a new connection is taken by run() at
        connect time, so starting with a sleep avoids an immediate duplicate.
        """
        while True:
            await asyncio.sleep(self.config.snapshot_seconds)
            await self._snapshot_all()


if __name__ == "__main__":
    configure()
    # Narrow demo config: one symbol, and a short flush window so a periodic
    # flush is visible before the run ends. Real capture uses the defaults and
    # max_messages=None.
    demo = StreamConfig(
        symbols=["BTCUSDT"],
        kinds=["trades", "depth", "book_ticker"],
        flush_seconds=5,
    )
    asyncio.run(StreamCollector(demo).run(max_messages=200))
