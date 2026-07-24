"""Websocket capture loop for Binance streams."""

import asyncio
import json

import websockets

from qde.stream.config import StreamConfig
from qde.stream.parsers import now_ms, parse_message


class StreamCollector:
    """Connects to Binance, subscribes to the configured streams, and reads
    messages as they arrive.

    This step only establishes the live connection and surfaces each message.
    Parsing, buffering, and writing to bronze are added in later steps.
    """

    def __init__(self, config: StreamConfig):
        self.config = config
        self.count = 0

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

    def _handle(self, message: dict, received_at: int) -> None:
        """Route one message to its parser.

        Placeholder output for now; step 4 replaces the print with a buffer
        keyed by (kind, symbol).
        """
        self.count += 1
        kind, row = parse_message(message["stream"], message["data"], received_at)

        # Difference between local arrival and exchange emission: the feed's
        # end-to-end latency, only measurable because received_at is captured live.
        latency_ms = received_at - row["event_time"]

        if kind == "trades":
            detail = f"{row['price']} x {row['quantity']}"
        else:
            detail = f"{len(row['bids'])} bids / {len(row['asks'])} asks"
        print(f"[{self.count:>3}] {kind:<6} {row['symbol']:<8} lat={latency_ms:>4}ms  {detail}")


if __name__ == "__main__":
    # Narrow demo config: one symbol, so the output stays watchable while both
    # kinds exercise the router. Pass max_messages=None to run indefinitely.
    demo = StreamConfig(symbols=["BTCUSDT"], kinds=["trades", "depth"])
    asyncio.run(StreamCollector(demo).run(max_messages=20))
