"""Parsers turning raw Binance payloads into flat bronze rows.

Bronze keeps what the exchange sent. Prices and quantities stay as the exact
strings Binance transmits, and exchange timestamps stay as raw epoch
milliseconds; numeric casting and datetime conversion belong to the silver
layer, where a bug can be fixed and the data rebuilt from bronze.

The one field added here is `received_at` — local arrival time. Binance never
reports when a message reached this process, so it cannot be recovered after
the fact and must be stamped live.
"""

from datetime import datetime, timezone


def now_ms() -> int:
    """Return current UTC time as epoch milliseconds.

    Milliseconds match the unit Binance uses for its own timestamps, so
    latency is a plain subtraction rather than a unit conversion.
    """
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def parse_trade(data: dict, received_at: int) -> dict:
    """Flatten a Binance trade payload into a bronze row.

    Args:
        data: The "data" object of a combined-stream trade message.
        received_at: Local arrival time, epoch ms.

    Returns:
        Flat row. `trade_id` is monotonic per symbol and is what gap
        detection watches.
    """
    return {
        "symbol": data["s"],
        "trade_id": data["t"],
        "price": data["p"],            # string: exact digits, no float rounding
        "quantity": data["q"],         # string: same reason
        "trade_time": data["T"],       # when the trade occurred
        "event_time": data["E"],       # when Binance emitted the message
        "buyer_is_maker": data["m"],   # True means a sell order hit the bid
        "received_at": received_at,
    }


def parse_book_ticker(data: dict, received_at: int) -> dict:
    """Flatten a Binance book-ticker payload into a bronze row.

    Fires on every change to the best bid or ask, so it resolves top-of-book
    moves that the 100ms diff-depth stream aggregates away.

    The payload carries no exchange timestamp, which makes `received_at` the
    only time reference these rows will ever have; without it they cannot be
    placed on a timeline at all.

    Args:
        data: The "data" object of a combined-stream bookTicker message.
        received_at: Local arrival time, epoch ms.

    Returns:
        Flat row. `update_id` is the book's global update id: strictly
        increasing, but not by one per message, so gap detection checks
        ordering rather than contiguity.
    """
    return {
        "symbol": data["s"],
        "bid_price": data["b"],        # string: exact digits, no float rounding
        "bid_qty": data["B"],          # string: same reason
        "ask_price": data["a"],        # string: same reason
        "ask_qty": data["A"],          # string: same reason
        "update_id": data["u"],
        "received_at": received_at,
    }


def parse_depth(data: dict, received_at: int) -> dict:
    """Flatten a Binance diff-depth payload into a bronze row.

    One row per message. Bid and ask levels are preserved exactly as received
    (lists of [price, quantity] string pairs); the order book is reconstructed
    downstream from these deltas, not here.

    Args:
        data: The "data" object of a combined-stream depthUpdate message.
        received_at: Local arrival time, epoch ms.

    Returns:
        Flat row. `first_update_id`/`final_update_id` chain across consecutive
        messages and are what gap detection watches.
    """
    return {
        "symbol": data["s"],
        "first_update_id": data["U"],
        "final_update_id": data["u"],
        "event_time": data["E"],
        "bids": data["b"],
        "asks": data["a"],
        "received_at": received_at,
    }


def parse_depth_snapshot(data: dict, symbol: str, received_at: int) -> dict:
    """Flatten a Binance REST order-book snapshot into a bronze row.

    The diff-depth stream carries changes, not state, so it is meaningless
    without a starting picture of the book. This snapshot is that anchor.

    Args:
        data: Decoded JSON body of a /api/v3/depth response.
        symbol: Canonical symbol. The response omits it, since it was supplied
            in the request, so it is passed in to keep the row self-contained.
        received_at: Local arrival time, epoch ms.

    Returns:
        Flat row. `last_update_id` is the anchor point: when the book is
        replayed, diff messages at or below it are already reflected here and
        are discarded.
    """
    return {
        "symbol": symbol,
        "last_update_id": data["lastUpdateId"],
        "bids": data["bids"],
        "asks": data["asks"],
        "received_at": received_at,
    }


def parse_message(stream: str, data: dict, received_at: int) -> tuple[str, dict]:
    """Route a combined-stream message to the parser for its kind.

    Args:
        stream: The message's "stream" field, e.g. "btcusdt@depth@100ms".
        data: The message's "data" payload.
        received_at: Local arrival time, epoch ms.

    Returns:
        (kind, row) where kind is the partition key, "trades" or "depth".

    Raises:
        ValueError: If the stream name matches no known kind.
    """
    # Depth streams carry a speed suffix ("...@depth@100ms"), so the marker is
    # matched anywhere in the name rather than at the end.
    if stream.endswith("@trade"):
        return "trades", parse_trade(data, received_at)
    if stream.endswith("@bookTicker"):
        return "book_ticker", parse_book_ticker(data, received_at)
    if "@depth" in stream:
        return "depth", parse_depth(data, received_at)
    raise ValueError(f"Unrecognised stream: {stream!r}")
