"""Venue-neutral websocket capture loop.

The exchange-specific dialect — stream naming, message decoding, and how the
order book is anchored — lives behind a `VenueAdapter` (see `qde.stream.venues`).
This loop owns only what every venue shares: buffering, timed flush, reconnect
with backoff, sequence-gap tracking, session markers, and the bronze layout.
"""

import asyncio
import json
import time
from collections import defaultdict
from datetime import UTC, datetime

import pandas as pd
import websockets
from websockets.exceptions import WebSocketException

from qde.log import configure, get_logger
from qde.stream.config import StreamConfig
from qde.stream.gaps import (
    HANDOVER,
    SESSION_START,
    SESSION_STOP,
    SequenceTracker,
    handover_record,
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

# How long to wait for the peer's half of the closing handshake before dropping the
# socket. The `websockets` default is 10s, and **Binance never replies to a close
# frame** — measured: connect costs ~1.2s, the close always burns the timeout in
# full, so the default turned every recycle into a ~9s hole instead of ~2s. This is
# a cap, not a delay: a venue that does reply closes immediately. Nothing is at risk
# in abandoning the handshake, because buffered rows are flushed before the close.
CLOSE_TIMEOUT_SECONDS = 2

# Frames `websockets` will hold for us before it stops reading the socket and lets
# TCP back-pressure build. The library default is 16, which two things make far too
# small: a handover leans entirely on the successor buffering everything that
# arrives while the predecessor closes (seconds of a combined stream — hundreds of
# frames), and `flush()` blocks the event loop, so any slow write eats the queue.
# Over-running it does not raise; the socket simply stops draining, which is how a
# venue decides you are a slow consumer and disconnects you. Frames are small, so
# the headroom costs a few MB at worst.
RECEIVE_QUEUE_FRAMES = 8192

# How long a retiring connection may go quiet before its backlog is judged empty.
# Comfortably longer than the gap between messages on a live stream (BTCUSDT
# top-of-book alone runs ~27/s) so an idle window really does mean "caught up",
# and short enough that a genuinely silent stream cannot stall the handover.
DRAIN_IDLE_SECONDS = 0.25

# A receive that takes longer than this had nothing waiting for it, so the backlog
# is spent. Well under the spacing of a live stream (~37ms between top-of-book
# updates on BTCUSDT) and well above the cost of returning an already-queued frame.
DRAIN_LIVE_WAIT_SECONDS = 0.005


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
        # Per-stream floor of ids already captured from the connection being
        # replaced. Non-empty only during the brief overlap after a handover;
        # each stream drops out of it as soon as it delivers something new.
        self._overlap_floor: dict[tuple[str, str], int] = {}
        self._overlap_dupes: dict[tuple[str, str], int] = {}
        self._handover_at = 0

    @property
    def supports_overlap(self) -> bool:
        """Whether this capture may run two connections at once during a handover.

        Only if every configured kind carries a monotonic id, because the overlap
        is reconciled by discarding ids already seen. A capture including an
        unsequenced stream (Coinbase's `l2update`) cannot tell a replayed diff
        from a new one, so it must close before reopening and accept the gap.
        """
        return not (self.adapter.unsequenced_kinds & set(self.config.kinds))

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
        conn = None  # the live connection's context manager, None when between them
        ws = None
        connected_at = 0
        # Set whenever the socket being read changes, from either a reconnect or a
        # handover. The ritual below must run for both, and running it exactly once
        # per connection is what keeps the anchoring invariant true.
        fresh_connection = False

        try:
            # Outer loop: a dropped connection is expected, not exceptional.
            while True:
                if ws is None:
                    try:
                        conn, ws = await self._connect(url)
                    except (WebSocketException, OSError) as exc:
                        # Buffered rows are written before waiting, so an outage
                        # never sits on top of unsaved data.
                        if self._disconnected_at is None:
                            self._disconnected_at = now_ms()
                        self.flush()
                        log.warning(
                            "connection_lost", error=type(exc).__name__, retry_in_s=backoff
                        )
                        await asyncio.sleep(backoff)
                        # Exponential backoff, capped: a long outage must not turn
                        # into a tight reconnect loop against the exchange.
                        backoff = min(backoff * 2, 60)
                        continue
                    backoff = 1  # reset only after a connection succeeds
                    fresh_connection = True

                if fresh_connection:
                    fresh_connection = False
                    # Records the preceding outage, if there was one, and resets
                    # sequence tracking so the first message of a new connection is
                    # a baseline rather than a spurious jump. After an *overlapped*
                    # handover there is no outage, so this is deliberately a no-op:
                    # tracking continues across the seam, which means a handover
                    # that did lose data would still be caught as a sequence jump.
                    self._on_connected()

                    # Anchor this connection before consuming its deltas. The
                    # socket is already open and buffering, so no diffs are lost
                    # while the snapshot is fetched; any that predate
                    # last_update_id are discarded on replay. A no-op for venues
                    # that deliver the snapshot inline.
                    await self._snapshot_all()
                    connected_at = now_ms()

                try:
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
                            result = await self._handover(url, conn, ws, received_at)
                            if result is None:
                                # Predecessor closed, successor failed: fall through
                                # to the reconnect path with its backoff.
                                conn, ws = None, None
                                break
                            next_conn, next_ws = result
                            connected_at = now_ms()
                            if next_ws is ws:
                                # Handover declined; the current socket is fine.
                                # Restarting the clock defers the next attempt by a
                                # full cycle rather than retrying on every message.
                                continue
                            conn, ws = next_conn, next_ws
                            fresh_connection = True
                            break  # re-enter the loop reading the successor
                    else:
                        # The iterator ended without raising: the peer closed the
                        # socket cleanly. That is still an outage — and it used to
                        # be recorded as nothing at all, because only the except
                        # branch below set the marker.
                        self._disconnected_at = now_ms()
                        self.flush()
                        log.warning("connection_closed_by_peer")
                        ws = None

                except (WebSocketException, OSError) as exc:
                    self._disconnected_at = now_ms()
                    self.flush()
                    log.warning("connection_lost", error=type(exc).__name__, retry_in_s=backoff)
                    ws = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
        finally:
            if conn is not None:
                await self._close(conn)
            # Runs on normal exit, on error, and on Ctrl-C. Without this final
            # flush everything buffered since the last interval would be lost,
            # and streamed data cannot be re-fetched. A best-effort stop marker
            # closes the session; a hard kill (SIGKILL) skips it, in which case
            # the next start marker still bounds the downtime.
            flush_task.cancel()
            snapshot_task.cancel()
            # An overlap still open at shutdown would otherwise lose its record.
            self._close_out_overlap()
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

        Cycling on our own schedule replaces that with a handover we control. Where
        the venue's streams are all sequenced this costs **nothing**: the successor
        is opened first and buffers through the predecessor's close, and the
        resulting overlap is discarded by id (see :meth:`_handover`). Where a stream
        cannot be de-duplicated the fallback is close-then-reopen, which does leave a
        hole — recorded honestly as a ``reconnect``.

        Reuses the caller's receive timestamp rather than reading the clock again —
        this runs once per message, and the message already carries the time.
        """
        limit = self.config.max_connection_seconds
        return limit > 0 and (now - connected_at) >= limit * 1000

    async def _connect(self, url: str):
        """Open one connection and complete its subscription handshake.

        Returns the context manager alongside the socket so the caller can hold
        two connections at once during a handover — which ``async with`` cannot
        express, since the successor must outlive the block that opened it.
        """
        cm = websockets.connect(
            url,
            # max_size follows the venue: Coinbase's inline book snapshot is
            # >1 MiB and would trip the websockets default frame limit.
            max_size=self.adapter.max_frame_bytes,
            close_timeout=CLOSE_TIMEOUT_SECONDS,
            max_queue=RECEIVE_QUEUE_FRAMES,
        )
        ws = await cm.__aenter__()
        try:
            # Some venues (Coinbase) subscribe with a frame after connecting;
            # Binance encodes it in the URL, so this is empty.
            for frame in self.adapter.subscribe_frames(self.config):
                await ws.send(json.dumps(frame))
        except BaseException:
            # Never leak a half-subscribed socket back to the caller.
            await self._close(cm)
            raise
        return cm, ws

    async def _close(self, cm) -> None:
        """Close a connection, tolerating a peer that will not shake hands.

        Closing is best-effort by design: the data is already buffered locally,
        so a failure here costs nothing and must not propagate into the capture
        loop — least of all during a handover, where the successor is already
        carrying the feed.
        """
        try:
            await cm.__aexit__(None, None, None)
        except Exception as exc:
            log.warning("close_failed", error=type(exc).__name__)

    async def _handover(self, url: str, current_cm, current_ws, at_ms: int):
        """Replace the live connection before the venue can silently degrade it.

        Where the venue allows it this costs nothing, and the reason is the
        ordering: the successor is connected, subscribed and **already filling
        its own receive buffer** while the predecessor is still being closed. The
        window that used to be a hole is now covered by both sockets rather than
        neither. What is left is not a gap but an overlap, and an overlap is
        recoverable — every message in it carries an id already seen, so it is
        discarded on arrival by :meth:`_is_overlap_duplicate`.

        Returns the connection to read next:

        * a new ``(cm, ws)`` — the handover happened and the predecessor is closed;
        * ``(current_cm, current_ws)`` — the successor could not be opened, so the
          working connection is kept and untouched. A failed handover must never
          cost the feed that is currently fine;
        * ``None`` — the predecessor was closed and the successor failed, so the
          capture is genuinely disconnected and the caller must reconnect.
        """
        if not self.supports_overlap:
            # An unsequenced stream cannot be de-duplicated, so overlapping would
            # write replayed diffs indistinguishable from new ones — and a diff
            # replayed out of order silently rewinds the book. Close first, accept
            # the hole, and record it honestly as the outage it is.
            log.info("connection_recycle", mode="close_then_reopen")
            self._disconnected_at = at_ms
            self.flush()
            await self._close(current_cm)
            try:
                return await self._connect(url)
            except (WebSocketException, OSError) as exc:
                log.warning("reopen_failed", error=type(exc).__name__, detail=str(exc))
                return None

        try:
            successor = await self._connect(url)
        except (WebSocketException, OSError) as exc:
            # The predecessor is untouched and still delivering.
            log.warning("handover_failed", error=type(exc).__name__, detail=str(exc))
            return current_cm, current_ws

        # The successor only covers what arrived AFTER it connected, so the
        # predecessor must be read up to at least that instant or the connect
        # window becomes a hole. It is not idle time: the predecessor kept
        # receiving throughout, into a buffer nobody was draining, and closing it
        # now would discard exactly the messages the successor never saw. Measured
        # cost of skipping this: a real sequence jump on every handover.
        drained = await self._drain(current_ws)

        # Arm the overlap filter from the predecessor's FINAL positions — after the
        # drain, so anything just recovered counts as already captured.
        self._arm_overlap(at_ms)
        await self._close(current_cm)

        log.info(
            "connection_handover",
            mode="overlapped",
            after_s=self.config.max_connection_seconds,
            streams=len(self._overlap_floor),
            drained=drained,
        )
        return successor

    async def _drain(self, ws) -> int:
        """Empty a retiring connection's backlog, up to the present moment.

        "Caught up" is detected by **how long each receive waits**, not by any
        timestamp. Rows are stamped when read rather than when they arrived, so a
        message recovered from a backlog is indistinguishable by its own clock from
        one that just landed; comparing ``received_at`` to the successor's connect
        time stopped the drain after exactly one message and left the hole this
        exists to close. Waiting for the socket to fall idle does not work either —
        on a stream delivering tens of messages a second the next one always
        arrives before any sane idle window expires.

        What does distinguish them: a queued message is returned immediately, while
        a live one costs the wait until the venue sends it. So drain while receives
        come back instantly, and stop at the first one that had to wait — that is
        the moment the backlog is spent and we are reading in real time. Real time
        is necessarily later than the successor's connect, so the two sockets
        together cover the whole timeline with an overlap in the middle.

        Bounded twice, because this socket is about to be discarded and must never
        be what stalls the capture: an idle timeout for a stream that has gone
        quiet, and a message ceiling for a firehose we cannot outpace. Hitting
        either just means a smaller overlap, which the sequence check then reports
        honestly rather than hides.
        """
        drained = 0
        while drained < RECEIVE_QUEUE_FRAMES:
            started = time.monotonic()
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=DRAIN_IDLE_SECONDS)
            except Exception:
                break  # empty, closed, or slow — all equally "caught up" here
            waited = time.monotonic() - started
            self._handle(json.loads(raw), now_ms())
            drained += 1
            if waited > DRAIN_LIVE_WAIT_SECONDS:
                break  # nothing was queued: we are reading live
        return drained

    def _arm_overlap(self, at_ms: int) -> None:
        """Freeze the predecessor's per-stream positions as the overlap floor."""
        # A previous overlap that never fully resolved (a stream went quiet before
        # delivering anything new) is closed out here rather than abandoned, so
        # every handover leaves a complete record behind it.
        self._close_out_overlap()
        self._overlap_floor = self.sequences.positions()
        self._overlap_dupes = dict.fromkeys(self._overlap_floor, 0)
        self._handover_at = at_ms

    def _close_out_overlap(self) -> None:
        """Emit handover records for any stream still inside the overlap window."""
        for kind, symbol in list(self._overlap_floor):
            self._finish_overlap(kind, symbol)

    def _finish_overlap(self, kind: str, symbol: str) -> None:
        """Retire one stream from the overlap and record what the overlap cost it.

        The duplicate count is only knowable once the stream has moved past the
        replayed window, which is why the record is written here rather than when
        the handover was initiated — at that point the honest answer would have
        been "unknown", and writing a zero would have been a guess dressed as a
        measurement.
        """
        key = (kind, symbol)
        self._overlap_floor.pop(key, None)
        duplicates = self._overlap_dupes.pop(key, 0)
        self._record_gap(handover_record(kind, symbol, self._handover_at, duplicates))

    def _is_overlap_duplicate(self, kind: str, row: dict) -> bool:
        """Was this row already captured from the connection being replaced?

        Only ever true during the moments after a handover. Each stream leaves the
        filter the instant it delivers something genuinely new, so this costs one
        dict lookup in steady state and cannot silently suppress data if a stream
        goes quiet mid-overlap.
        """
        if not self._overlap_floor:
            return False
        key = (kind, row["symbol"])
        floor = self._overlap_floor.get(key)
        if floor is None:
            return False

        current = self.adapter.dedup_key(kind, row)
        if current is None:
            # Unreachable unless a venue under-declares `unsequenced_kinds`. Accept
            # the row: capturing a duplicate is recoverable downstream, dropping
            # real data is not.
            log.warning("overlap_undedupable", kind=kind, symbol=row["symbol"])
            self._finish_overlap(kind, row["symbol"])
            return False

        if current <= floor:
            self._overlap_dupes[key] = self._overlap_dupes.get(key, 0) + 1
            return True

        self._finish_overlap(kind, row["symbol"])  # this stream is past the overlap
        return False

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
        # Discarded before buffering *and* before the sequence check: a duplicate
        # moves backwards, which the tracker would otherwise report as a jump.
        if self._is_overlap_duplicate(kind, row):
            return
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
        self.buffers[("gaps", gap["symbol"])].append(gap)

        if gap["reason"] == HANDOVER:
            # Shares the partition because it belongs to the same continuity
            # record, but it is not a gap: it is not counted toward the session's
            # gap total and it is not a warning, or every clean handover would
            # look like a fault in the logs.
            log.info(
                "handover",
                kind=gap["stream_kind"],
                symbol=gap["symbol"],
                duplicates=gap["duplicates"],
            )
            return

        self.gap_count += 1
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
