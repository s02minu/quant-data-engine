"""Websocket capture loop for Binance streams."""

import asyncio
import json

import websockets

from qde.stream.config import StreamConfig


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
                self._handle(json.loads(raw))
                if max_messages is not None and self.count >= max_messages:
                    break

    def _handle(self, message: dict) -> None:
        """Process one message. Placeholder: print a one-line summary."""
        self.count += 1
        stream = message.get("stream")
        data = message.get("data", {})
        print(f"[{self.count:>3}] {stream}: {data}")


if __name__ == "__main__":
    # Narrow demo config: one symbol, trades only, so the output is watchable.
    # Widen by editing symbols/kinds, or pass max_messages=None to run forever.
    demo = StreamConfig(symbols=["BTCUSDT"], kinds=["trades"])
    asyncio.run(StreamCollector(demo).run(max_messages=20))
