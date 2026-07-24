"""Websocket capture loop for Binance streams."""

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import websockets

from qde.stream.config import StreamConfig
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

        try:
            # opens the connection and guarantees the connection is cleanly closed
            async with websockets.connect(url) as ws:
                # Yields one message per push and pauses in between without blocking
                # the process; runs until the socket closes (or max_messages is hit).
                async for raw in ws:
                    # Stamped before any parsing work so the value reflects arrival,
                    # not how long processing happened to take.
                    received_at = now_ms()
                    self._handle(json.loads(raw), received_at)
                    if max_messages is not None and self.count >= max_messages:
                        break
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
        self.buffers[(kind, row["symbol"])].append(row)

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
