import asyncio
import json

import pandas as pd

import qde.stream.collector as collector_mod
from qde.stream.collector import SESSION_SYMBOL, StreamCollector
from qde.stream.config import StreamConfig
from qde.stream.gaps import RECONNECT, SEQUENCE_JUMP, SESSION_START, SESSION_STOP


def _trade_msg(trade_id):
    """A combined-stream trade frame, JSON-encoded as the socket delivers it."""
    return json.dumps(
        {
            "stream": "btcusdt@trade",
            "data": {
                "s": "BTCUSDT",
                "t": trade_id,
                "p": "100.0",
                "q": "1.0",
                "T": 1000,
                "E": 1000,
                "m": True,
            },
        }
    )


class _FakeConn:
    """Stand-in for websockets.connect(url): an async context manager whose
    entered value yields scripted messages, then optionally raises to simulate
    a dropped connection."""

    def __init__(self, messages, raise_exc=None):
        self._messages = messages
        self._raise_exc = raise_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self._stream()

    async def _stream(self):
        for message in self._messages:
            yield message
        if self._raise_exc is not None:
            raise self._raise_exc


def _fake_connect(scripts):
    """Return a connect() that plays one script per call, holding the last."""
    state = {"i": 0}

    def connect(url, **kwargs):
        # **kwargs absorbs max_size, which the collector now passes per venue.
        script = scripts[min(state["i"], len(scripts) - 1)]
        state["i"] += 1
        return _FakeConn(*script)

    return connect


def test_collector_reconnects_after_drop_and_records_gap(tmp_path, monkeypatch):
    # First connection delivers two trades then drops; the second delivers the
    # rest. OSError is what the collector's reconnect loop is written to catch.
    scripts = [
        ([_trade_msg(1), _trade_msg(2)], OSError("simulated drop")),
        ([_trade_msg(3), _trade_msg(4), _trade_msg(5)], None),
    ]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))

    cfg = StreamConfig(
        symbols=["BTCUSDT"],
        kinds=["trades"],
        base_dir=str(tmp_path),
        flush_seconds=100,
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=5))

    # It survived the drop and consumed the messages from the second connection.
    assert collector.count == 5
    # Exactly one gap: the reconnect. No spurious sequence jump across the seam.
    assert collector.gap_count == 1

    gaps = pd.read_parquet(
        tmp_path / "bronze/group=microstructure/source=binance/kind=gaps/symbol=BTCUSDT"
    )
    assert len(gaps) == 1
    assert gaps.iloc[0]["reason"] == RECONNECT


def test_collector_writes_bronze_parts(tmp_path, monkeypatch):
    scripts = [([_trade_msg(1), _trade_msg(2), _trade_msg(3)], None)]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))

    cfg = StreamConfig(symbols=["BTCUSDT"], kinds=["trades"], base_dir=str(tmp_path))
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=3))

    trades = pd.read_parquet(
        tmp_path / "bronze/group=microstructure/source=binance/kind=trades/symbol=BTCUSDT"
    )
    assert len(trades) == 3
    assert list(trades["trade_id"]) == [1, 2, 3]
    # Prices survived the round-trip as strings, not floats.
    assert trades.iloc[0]["price"] == "100.0"


def test_collector_records_session_start_and_stop(tmp_path, monkeypatch):
    scripts = [([_trade_msg(1), _trade_msg(2)], None)]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))

    cfg = StreamConfig(symbols=["BTCUSDT"], kinds=["trades"], base_dir=str(tmp_path))
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=2))

    session = pd.read_parquet(
        tmp_path
        / f"bronze/group=microstructure/source=binance/kind=session/symbol={SESSION_SYMBOL}"
    )
    events = list(session["event"])
    assert SESSION_START in events
    assert SESSION_STOP in events
    # The stop marker carries the session totals; start leaves them null.
    stop = session[session["event"] == SESSION_STOP].iloc[0]
    assert stop["message_count"] == 2


# --- deliberate connection recycling -------------------------------------------
#
# Binance drops a burst of messages on a connection open ~48h WITHOUT closing it
# (measured: 2,931 depth messages, six streams, same millisecond, no disconnect).
# The collector pre-empts that by cycling the connection on its own schedule.


def _fake_clock(monkeypatch, step_ms):
    """Replace the collector's clock with one that advances a fixed step per read."""
    state = {"t": 0}

    def now_ms():
        state["t"] += step_ms
        return state["t"]

    monkeypatch.setattr(collector_mod, "now_ms", now_ms)


def test_recycle_is_off_when_limit_is_zero():
    cfg = StreamConfig(symbols=["BTCUSDT"], kinds=["trades"], max_connection_seconds=0)
    collector = StreamCollector(cfg)
    # Even an absurdly old connection stays put when the feature is disabled.
    assert collector._should_recycle(connected_at=0, now=10**12) is False


def test_recycle_waits_for_the_full_age():
    cfg = StreamConfig(symbols=["BTCUSDT"], kinds=["trades"], max_connection_seconds=100)
    collector = StreamCollector(cfg)
    assert collector._should_recycle(connected_at=0, now=99_999) is False
    assert collector._should_recycle(connected_at=0, now=100_000) is True


def test_aged_connection_is_cycled_and_the_hole_is_recorded(tmp_path, monkeypatch):
    # Two connections' worth of script: the collector should abandon the first
    # one itself, without the socket ever erroring.
    scripts = [
        ([_trade_msg(1), _trade_msg(2), _trade_msg(3)], None),
        ([_trade_msg(4), _trade_msg(5), _trade_msg(6)], None),
    ]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))
    _fake_clock(monkeypatch, step_ms=600)

    cfg = StreamConfig(
        symbols=["BTCUSDT"],
        kinds=["trades"],
        base_dir=str(tmp_path),
        flush_seconds=100,
        max_connection_seconds=1,  # 1s, so the fake clock trips it after two reads
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=4))

    # It moved to the second connection on its own — the first never raised.
    assert collector.count == 4

    gaps = pd.read_parquet(
        tmp_path / "bronze/group=microstructure/source=binance/kind=gaps/symbol=BTCUSDT"
    )
    # The cycle is deliberate, but the ~2s hole it leaves is real and must be
    # recorded as a reconnect — the lake should not claim continuity it lacks.
    assert list(gaps["reason"]) == [RECONNECT]


def test_cycling_does_not_look_like_missed_data(tmp_path, monkeypatch):
    # The whole point is trading a silent sequence_jump for a known reconnect.
    # If the cycle itself logged a jump it would be a worse deal, not a better one.
    scripts = [([_trade_msg(1), _trade_msg(2)], None), ([_trade_msg(3), _trade_msg(4)], None)]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))
    _fake_clock(monkeypatch, step_ms=600)

    cfg = StreamConfig(
        symbols=["BTCUSDT"],
        kinds=["trades"],
        base_dir=str(tmp_path),
        flush_seconds=100,
        max_connection_seconds=1,
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=3))

    gaps = pd.read_parquet(
        tmp_path / "bronze/group=microstructure/source=binance/kind=gaps/symbol=BTCUSDT"
    )
    assert SEQUENCE_JUMP not in list(gaps["reason"])
