"""Coinbase adapter + parser tests, built from payloads captured live off
wss://ws-feed.exchange.coinbase.com (BTC-USD)."""

import asyncio
import json

import pandas as pd
import pytest

import qde.stream.collector as collector_mod
from qde.stream.config import StreamConfig
from qde.stream.gaps import SEQUENCE_JUMP, SequenceTracker
from qde.stream.venues import get_adapter
from qde.stream.venues.coinbase import (
    CoinbaseAdapter,
    parse_heartbeat,
    parse_l2update,
    parse_match,
    parse_snapshot,
    parse_ticker,
    to_canonical,
    to_native,
)

# --- live-captured payloads (trimmed book arrays) --------------------------

MATCH = {
    "type": "match",
    "trade_id": 1067894408,
    "maker_order_id": "86ff8add-b814-41c8-979e-abeab3c03a49",
    "taker_order_id": "9c938f8f-8457-4a83-bb83-05df00ed8590",
    "side": "buy",
    "size": "0.00000018",
    "price": "64542.45",
    "product_id": "BTC-USD",
    "sequence": 133874872462,
    "time": "2026-08-07T08:46:04.329337Z",
}
SNAPSHOT = {
    "type": "snapshot",
    "product_id": "BTC-USD",
    "time": "2026-08-07T08:46:04.363113Z",
    "bids": [["64542.45", "0.20226956"], ["64542.44", "0.04702050"]],
    "asks": [["64542.46", "0.04779027"], ["64542.48", "0.03000000"]],
}
L2UPDATE = {
    "type": "l2update",
    "product_id": "BTC-USD",
    "changes": [["buy", "63453.41", "0.00000000"], ["sell", "64548.32", "1.31168865"]],
    "time": "2026-08-07T08:46:04.390555Z",
}
TICKER = {
    "type": "ticker",
    "sequence": 133874872462,
    "product_id": "BTC-USD",
    "price": "64542.45",
    "open_24h": "64873.61",
    "volume_24h": "4147.32447406",
    "low_24h": "64087.41",
    "high_24h": "64932.11",
    "volume_30d": "167424.63913314",
    "best_bid": "64542.45",
    "best_bid_size": "0.20226956",
    "best_ask": "64542.46",
    "best_ask_size": "0.04779027",
    "side": "sell",
    "time": "2026-08-07T08:46:04.329337Z",
    "trade_id": 1067894408,
    "last_size": "0.00000018",
}
HEARTBEAT = {
    "type": "heartbeat",
    "last_trade_id": 1067894409,
    "product_id": "BTC-USD",
    "sequence": 133874872716,
    "time": "2026-08-07T08:46:05.000000Z",
}


# --- symbol translation ----------------------------------------------------

def test_symbol_maps_usdt_to_usd_and_back():
    assert to_native("BTCUSDT") == "BTC-USD"
    assert to_native("ETHUSDT") == "ETH-USD"
    # Coinbase's USD product lands under the same canonical symbol as Binance's
    # USDT one, so cross-venue queries join on `symbol`.
    assert to_canonical("BTC-USD") == "BTCUSDT"
    assert to_canonical("SOL-USD") == "SOLUSDT"


# --- parsers ---------------------------------------------------------------

def test_parse_match_normalises_to_bronze_contract():
    row = parse_match(MATCH, received_at=999)
    assert row["symbol"] == "BTCUSDT"  # canonicalised from BTC-USD
    assert row["trade_id"] == 1067894408
    assert row["price"] == "64542.45"  # kept as an exact string
    assert row["quantity"] == "0.00000018"  # Coinbase "size" -> "quantity"
    assert isinstance(row["price"], str)
    assert row["sequence"] == 133874872462
    assert row["received_at"] == 999


def test_parse_ticker_aligns_top_of_book_field_names():
    row = parse_ticker(TICKER, received_at=5)
    # Shares Binance's book-ticker names so the two venues line up.
    assert row["bid_price"] == "64542.45"
    assert row["ask_price"] == "64542.46"
    assert row["bid_qty"] == "0.20226956"
    # sequence drives the ordering gap check via update_id.
    assert row["update_id"] == 133874872462
    assert row["symbol"] == "BTCUSDT"


def test_parse_snapshot_keeps_full_book_and_has_no_update_id():
    row = parse_snapshot(SNAPSHOT, received_at=1)
    assert row["bids"][0] == ["64542.45", "0.20226956"]
    assert "last_update_id" not in row  # Coinbase gives no anchor id
    assert row["symbol"] == "BTCUSDT"


def test_parse_l2update_keeps_changes_and_is_unsequenced():
    row = parse_l2update(L2UPDATE, received_at=2)
    assert row["changes"][0] == ["buy", "63453.41", "0.00000000"]
    # No update ids: depth is not gap-checkable per message on Coinbase.
    assert "final_update_id" not in row
    assert "first_update_id" not in row


def test_parse_heartbeat_carries_sequence_and_last_trade_id():
    row = parse_heartbeat(HEARTBEAT, received_at=3)
    assert row["last_trade_id"] == 1067894409
    assert row["sequence"] == 133874872716
    assert row["symbol"] == "BTCUSDT"


# --- adapter ---------------------------------------------------------------

def test_get_adapter_resolves_coinbase():
    assert isinstance(get_adapter("coinbase"), CoinbaseAdapter)


def test_route_dispatches_each_message_type():
    a = CoinbaseAdapter()
    assert a.route(MATCH, 1)[0] == "trades"
    assert a.route(SNAPSHOT, 1)[0] == "snapshot"
    assert a.route(L2UPDATE, 1)[0] == "depth"
    assert a.route(TICKER, 1)[0] == "book_ticker"
    assert a.route(HEARTBEAT, 1)[0] == "heartbeat"


def test_route_ignores_subscription_ack_and_raises_on_error():
    a = CoinbaseAdapter()
    assert a.route({"type": "subscriptions", "channels": []}, 1) is None
    with pytest.raises(ValueError, match="Coinbase feed error"):
        a.route({"type": "error", "message": "bad product"}, 1)


def test_subscribe_frame_maps_kinds_to_channels_and_always_adds_heartbeat():
    a = CoinbaseAdapter()
    cfg = StreamConfig(
        source="coinbase", symbols=["BTCUSDT"], kinds=["trades", "depth", "book_ticker"]
    )
    frame = a.subscribe_frames(cfg)[0]
    assert frame["type"] == "subscribe"
    assert frame["product_ids"] == ["BTC-USD"]
    assert set(frame["channels"]) == {"matches", "level2_batch", "ticker", "heartbeat"}


def test_max_frame_bytes_exceeds_the_snapshot_size():
    # The captured snapshot was ~1.19 MB; the cap must clear it with headroom.
    assert CoinbaseAdapter().max_frame_bytes > 2 * 2**20


def test_coinbase_uses_a_fixed_feed_url_not_the_binance_default():
    cfg = StreamConfig(source="coinbase", symbols=["BTCUSDT"])
    assert get_adapter("coinbase").ws_url(cfg).startswith("wss://ws-feed.exchange.coinbase.com")


# --- continuity ------------------------------------------------------------

def test_unsequenced_depth_is_skipped_not_crashed():
    # A Coinbase depth row has no update ids; the tracker must return None,
    # never KeyError.
    tracker = SequenceTracker()
    assert tracker.check("depth", "BTCUSDT", parse_l2update(L2UPDATE, 1)) is None


def test_trade_id_gap_is_detected_on_coinbase_trades():
    tracker = SequenceTracker()
    first = parse_match({**MATCH, "trade_id": 100}, 1)
    skipped = parse_match({**MATCH, "trade_id": 102}, 2)
    assert tracker.check("trades", "BTCUSDT", first) is None  # baseline
    gap = tracker.check("trades", "BTCUSDT", skipped)
    assert gap is not None
    assert gap["reason"] == SEQUENCE_JUMP
    assert gap["missing_count"] == 1


# --- collector integration (fake socket) -----------------------------------

class _FakeCoinbaseConn:
    """A Coinbase-shaped fake: accepts a subscribe frame, then yields scripted
    JSON messages as the socket would."""

    def __init__(self, messages):
        self._messages = messages
        self.sent: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, data):
        self.sent.append(data)

    def __aiter__(self):
        return self._stream()

    async def _stream(self):
        for message in self._messages:
            yield message


def test_collector_captures_all_coinbase_kinds_to_bronze(tmp_path, monkeypatch):
    # Ack, then one of each kind; two extra matches make the tape three trades.
    messages = [
        json.dumps({"type": "subscriptions", "channels": []}),
        json.dumps({**MATCH, "type": "last_match", "trade_id": 100}),
        json.dumps({**MATCH, "trade_id": 101}),
        json.dumps({**MATCH, "trade_id": 102}),
        json.dumps(SNAPSHOT),
        json.dumps(L2UPDATE),
        json.dumps(TICKER),
        json.dumps(HEARTBEAT),
    ]
    monkeypatch.setattr(
        collector_mod.websockets, "connect", lambda url, **kw: _FakeCoinbaseConn(messages)
    )

    cfg = StreamConfig(
        source="coinbase",
        symbols=["BTCUSDT"],
        kinds=["trades", "depth", "book_ticker"],
        base_dir=str(tmp_path),
        flush_seconds=100,
    )
    collector = collector_mod.StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=len(messages)))

    # Contiguous trade ids -> no gaps.
    assert collector.gap_count == 0

    base = tmp_path / "bronze/group=microstructure/source=coinbase"
    trades = pd.read_parquet(base / "kind=trades/symbol=BTCUSDT")
    assert list(trades["trade_id"]) == [100, 101, 102]
    assert trades.iloc[0]["price"] == "64542.45"  # string round-trip

    # Every kind reached its own partition, under source=coinbase / canonical symbol.
    assert (base / "kind=depth/symbol=BTCUSDT").exists()
    assert (base / "kind=snapshot/symbol=BTCUSDT").exists()
    assert (base / "kind=book_ticker/symbol=BTCUSDT").exists()
    assert (base / "kind=heartbeat/symbol=BTCUSDT").exists()

    # The subscribe frame was actually sent.
    assert collector.count == len(messages)
