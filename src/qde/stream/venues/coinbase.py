"""Coinbase Exchange adapter and payload parsers.

Coinbase's public market-data feed differs from Binance in three ways that this
module absorbs so the collector stays dialect-free:

    * **Subscription** is a frame sent after connecting, not a URL query.
    * **The order book anchors inline.** Subscribing to `level2_batch` yields one
      full `snapshot` (>1 MiB — hence the raised `max_frame_bytes`) then batched
      `l2update` diffs, so no REST snapshot is needed; the book re-anchors on
      every (re)connect.
    * **Diffs are unsequenced.** `l2update` carries no update id, so per-message
      depth continuity is not possible; trades stay contiguous by `trade_id`,
      the ticker orders by `sequence`, and the `heartbeat` channel (captured as
      its own kind) is the liveness beacon for the quiet-market case.

Channels used are all public/no-auth: `matches` (trades), `level2_batch`
(the batched book — the full `level2` channel now requires auth), `ticker`
(top of book), and `heartbeat`.

Bronze fidelity is preserved venue-by-venue: Coinbase sends prices/sizes as
strings and timestamps as ISO-8601, and they are stored exactly as sent — the
silver layer casts and reconciles, per source.
"""

from qde.stream.config import StreamConfig
from qde.stream.venues.base import VenueAdapter

FEED_URL = "wss://ws-feed.exchange.coinbase.com"

# One capture kind -> the Coinbase channel that carries it. `heartbeat` is added
# unconditionally in subscribe_frames.
_CHANNEL_FOR_KIND = {
    "trades": "matches",
    "depth": "level2_batch",
    "book_ticker": "ticker",
}


def to_native(canonical: str) -> str:
    """Canonical BTCUSDT -> Coinbase BTC-USD.

    Coinbase quotes in USD, not USDT; that USD/USDT gap is exactly the
    cross-venue basis this second venue exists to capture.
    """
    base = canonical.removesuffix("USDT")
    return f"{base}-USD"


def to_canonical(product_id: str) -> str:
    """Coinbase BTC-USD -> canonical BTCUSDT.

    So Coinbase rows land in the same `symbol=` partition as Binance's and
    cross-venue queries join on `symbol`, with `source=` distinguishing the
    book. Mirrors how the bars layer already maps Coinbase.
    """
    base = product_id.split("-", 1)[0]
    return f"{base}USDT"


def parse_match(data: dict, received_at: int) -> dict:
    """Flatten a Coinbase `match`/`last_match` (a trade) into a bronze row.

    `trade_id` is contiguous per product and is what gap detection watches.
    `side` is the resting (maker) side as Coinbase reports it, kept raw — the
    silver layer derives aggressor direction rather than the bronze boundary.
    """
    return {
        "symbol": to_canonical(data["product_id"]),
        "trade_id": data["trade_id"],
        "price": data["price"],  # string: exact digits, no float rounding
        "quantity": data["size"],  # Binance's "quantity"; kept as a string
        "side": data["side"],
        "trade_time": data["time"],  # ISO-8601 string, exactly as sent
        "sequence": data["sequence"],  # per-product message sequence
        "received_at": received_at,
    }


def parse_ticker(data: dict, received_at: int) -> dict:
    """Flatten a Coinbase `ticker` into a bronze top-of-book row.

    Top-of-book fields share Binance's book-ticker names (`bid_price`, ...) so
    the two venues' books line up for cross-venue comparison; `update_id` is the
    per-product `sequence`, strictly increasing, so gap detection checks
    ordering. The extra last-trade fields Coinbase bundles in are kept as-is.
    """
    return {
        "symbol": to_canonical(data["product_id"]),
        "bid_price": data["best_bid"],
        "bid_qty": data["best_bid_size"],
        "ask_price": data["best_ask"],
        "ask_qty": data["best_ask_size"],
        "update_id": data["sequence"],
        "price": data["price"],  # last trade price
        "last_size": data["last_size"],
        "trade_id": data["trade_id"],
        "side": data["side"],
        "event_time": data["time"],
        "received_at": received_at,
    }


def parse_snapshot(data: dict, received_at: int) -> dict:
    """Flatten a Coinbase `level2` snapshot into a bronze row.

    The whole book arrives inline as [price, size] string pairs; there is no
    update id to anchor against (unlike Binance's `lastUpdateId`), so the diffs
    that follow are applied by arrival order after this snapshot.
    """
    return {
        "symbol": to_canonical(data["product_id"]),
        "bids": data["bids"],
        "asks": data["asks"],
        "snapshot_time": data.get("time"),  # present on the live feed; optional historically
        "received_at": received_at,
    }


def parse_l2update(data: dict, received_at: int) -> dict:
    """Flatten a Coinbase `l2update` (an order-book diff) into a bronze row.

    `changes` is a list of [side, price, size] string triples; a size of "0"
    means the level was removed. Carries no sequence/update id, so it is not
    gap-checkable per message.
    """
    return {
        "symbol": to_canonical(data["product_id"]),
        "changes": data["changes"],
        "event_time": data["time"],
        "received_at": received_at,
    }


def parse_heartbeat(data: dict, received_at: int) -> dict:
    """Flatten a Coinbase `heartbeat` into a bronze row.

    Fires once per second per product, carrying the current per-product
    `sequence` and `last_trade_id`. It is the only signal that separates a
    silent feed from a quiet market, and lets the silver layer reconcile drops.
    """
    return {
        "symbol": to_canonical(data["product_id"]),
        "last_trade_id": data["last_trade_id"],
        "sequence": data["sequence"],
        "heartbeat_time": data["time"],
        "received_at": received_at,
    }


class CoinbaseAdapter(VenueAdapter):
    """Coinbase Exchange public market-data capture.

    Connects to a fixed feed URL and subscribes with a frame; anchors the book
    from the inline snapshot rather than REST; normalises every message to the
    same bronze contract Binance uses, canonical symbol and all.
    """

    name = "coinbase"

    def native_symbol(self, canonical: str) -> str:
        return to_native(canonical)

    def ws_url(self, config: StreamConfig) -> str:
        return FEED_URL

    @property
    def max_frame_bytes(self) -> int:
        # The full L2 snapshot for a liquid pair is >1 MiB and trips the
        # websockets default limit; 16 MiB leaves headroom for a deeper book.
        return 16 * 2**20

    @property
    def unsequenced_kinds(self) -> frozenset[str]:
        # `l2update` carries no update id (only a wall-clock `time`), so two
        # copies of the same diff are indistinguishable from two genuine diffs.
        # That rules out overlapping connections for a capture including depth:
        # duplicated absolute level-sizes are idempotent, but replaying an OLD
        # one after a NEWER one silently rewinds the book, and arrival order
        # across two sockets is not guaranteed. The inline snapshot rides on the
        # same channel, so it is covered by naming depth here. Such a capture
        # falls back to close-then-reopen, which for this venue is sub-second
        # anyway (measured 0.82s) because Coinbase answers the close handshake.
        return frozenset({"depth"})

    def subscribe_frames(self, config: StreamConfig) -> list[dict]:
        channels = [_CHANNEL_FOR_KIND[k] for k in config.kinds if k in _CHANNEL_FOR_KIND]
        channels.append("heartbeat")  # always on: the liveness/continuity beacon
        return [
            {
                "type": "subscribe",
                "product_ids": [self.native_symbol(s) for s in config.symbols],
                "channels": channels,
            }
        ]

    def route(self, message: dict, received_at: int) -> tuple[str, dict] | None:
        kind = message.get("type")
        if kind in ("match", "last_match"):
            return "trades", parse_match(message, received_at)
        if kind == "snapshot":
            return "snapshot", parse_snapshot(message, received_at)
        if kind == "l2update":
            return "depth", parse_l2update(message, received_at)
        if kind == "ticker":
            return "book_ticker", parse_ticker(message, received_at)
        if kind == "heartbeat":
            return "heartbeat", parse_heartbeat(message, received_at)
        if kind == "error":
            # A rejected subscription would otherwise leave the socket silently
            # idle; surface it loudly instead.
            raise ValueError(f"Coinbase feed error: {message.get('message')}")
        return None  # "subscriptions" ack and anything else uncaptured
