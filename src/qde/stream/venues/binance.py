"""Binance adapter: the original capture dialect, unchanged behind the seam.

The Binance-specific logic already lived in `config.stream_names()` (URL naming)
and `parsers` (payload decoding); this adapter simply presents it through the
`VenueAdapter` interface, so the collector no longer speaks Binance directly.
Behaviour is identical to the pre-seam collector.
"""

from qde.loaders.http import get_with_requests
from qde.stream.config import StreamConfig
from qde.stream.parsers import parse_depth_snapshot, parse_message
from qde.stream.venues.base import VenueAdapter


class BinanceAdapter(VenueAdapter):
    """Binance combined-stream capture.

    Subscriptions are multiplexed onto one connection via the combined endpoint
    (`/stream?streams=a/b/c`); each message arrives wrapped as
    `{"stream": <name>, "data": <payload>}`. The diff-depth stream carries
    changes only, so the book is anchored with periodic REST snapshots.
    """

    name = "binance"

    def native_symbol(self, canonical: str) -> str:
        """Binance's REST symbol is the canonical form as-is (BTCUSDT).

        (The websocket lowercases it, but only inside `stream_names`; the REST
        depth endpoint takes the uppercase form.)
        """
        return canonical

    def ws_url(self, config: StreamConfig) -> str:
        """Combined-stream URL with every configured subscription in the query."""
        streams = "/".join(config.stream_names())
        return f"{config.ws_base_url}/stream?streams={streams}"

    @property
    def rest_snapshots(self) -> bool:
        return True

    def fetch_snapshot(self, config: StreamConfig, symbol: str, received_at: int) -> dict:
        """One order-book snapshot over REST, reusing the batch retry helper."""
        response = get_with_requests(
            f"{config.rest_base_url}/api/v3/depth",
            params={"symbol": self.native_symbol(symbol), "limit": config.snapshot_depth},
        )
        return parse_depth_snapshot(response.json(), symbol, received_at)

    def route(self, message: dict, received_at: int) -> tuple[str, dict]:
        """Route a combined-stream message to the parser for its kind."""
        return parse_message(message["stream"], message["data"], received_at)
