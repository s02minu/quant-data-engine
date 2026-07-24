"""Websocket capture loop for Binance streams."""

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import websockets
from websockets.exceptions import WebSocketException

from qde.stream.config import StreamConfig
from qde.stream.gaps import SequenceTracker, reconnect_gap
from qde.stream.parsers import now_ms, parse_message
from qde.stream.paths import bronze_path


class StreamCollector:
    """Connects to Binance, subscribes to the configured streams, and reads
    messages as they arrive.

    This step only establishes the live connection and surfaces each message.
    Parsing, buffering, and writing to bronze are added in later steps.
    """

    def __init__(self, config: StreamConfig):
        self.config = config
        self.count = 0
        # Rows wait here between flushes. Keyed by (kind, symbol) because that
        # pair identifies one bronze partition, so a buffer maps to one file.
        self.buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self.sequences = SequenceTracker()
        self.gap_count = 0
        self._disconnected_at: int | None = None

    def _combined_url(self) -> str:
        """Build the combined-stream URL for all configured subscriptions.

        Binance exposes many streams over one connection via the combined
        endpoint: /stream?streams=<a>/<b>/<c>. Messages then arrive wrapped as
        {"stream": <name>, "data": <payload>}.
        """
        # All subscriptions are multiplexed onto one connection; each incoming
        # message is then tagged with its origin in the "stream" field.
        streams = "/".join(self.config.stream_names())
        return f"{self.config.ws_base_url}/stream?streams={streams}"

    async def run(self, max_messages: int | None = None) -> None:
        """Open the connection and read messages until stopped.

        Args:
            max_messages: Stop after this many messages. Use None to run
                indefinitely (the real capture mode); a small number is handy
                for a bounded demo.
        """
        url = self._combined_url()

        # The second concurrent task: it sleeps between flushes while the read
        # loop below keeps draining the socket. An unread socket would back up
        # and eventually be dropped by the exchange.
        flush_task = asyncio.create_task(self._flush_loop())

        backoff = 1

        try:
            # Outer loop: a dropped connection is expected, not exceptional.
            while True:
                try:
                    # opens the connection and guarantees the connection is cleanly closed
                    async with websockets.connect(url) as ws:
                        self._on_connected()
                        backoff = 1  # reset only after a connection succeeds

                        # Yields one message per push and pauses in between without
                        # blocking the process; runs until the socket closes.
                        async for raw in ws:
                            # Stamped before any parsing work so the value reflects
                            # arrival, not how long processing happened to take.
                            received_at = now_ms()
                            self._handle(json.loads(raw), received_at)
                            if max_messages is not None and self.count >= max_messages:
                                return

                except (WebSocketException, OSError) as exc:
                    # Buffered rows are written before waiting, so an outage never
                    # sits on top of unsaved data.
                    self._disconnected_at = now_ms()
                    self.flush()
                    print(f"connection lost ({type(exc).__name__}); retrying in {backoff}s")
                    await asyncio.sleep(backoff)
                    # Exponential backoff, capped: a long outage must not turn into
                    # a tight reconnect loop against the exchange.
                    backoff = min(backoff * 2, 60)
        finally:
            # Runs on normal exit, on error, and on Ctrl-C. Without this final
            # flush everything buffered since the last interval would be lost,
            # and streamed data cannot be re-fetched.
            flush_task.cancel()
            self.flush()

    def _handle(self, message: dict, received_at: int) -> None:
        """Route one message to its parser.

        Placeholder output for now; step 4 replaces the print with a buffer
        keyed by (kind, symbol).
        """
        self.count += 1
        kind, row = parse_message(message["stream"], message["data"], received_at)
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
        missing = gap["missing_count"]
        detail = f"{missing} missing" if missing is not None else "count unknown"
        print(f"GAP  {gap['stream_kind']}/{gap['symbol']}  {gap['reason']}  ({detail})")

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
        batch_time = datetime.now(timezone.utc)
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
            print(f"flushed {len(rows):>5} rows -> {path}")

        return written

    async def _flush_loop(self) -> None:
        """Flush on a fixed interval, concurrently with the read loop."""
        while True:
            await asyncio.sleep(self.config.flush_seconds)
            self.flush()


if __name__ == "__main__":
    # Narrow demo config: one symbol, and a short flush window so a periodic
    # flush is visible before the run ends. Real capture uses the defaults and
    # max_messages=None.
    demo = StreamConfig(
        symbols=["BTCUSDT"],
        kinds=["trades", "depth", "book_ticker"],
        flush_seconds=5,
    )
    asyncio.run(StreamCollector(demo).run(max_messages=200))
