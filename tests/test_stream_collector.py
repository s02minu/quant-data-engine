import asyncio
import json

import pandas as pd

import qde.stream.collector as collector_mod
from qde.stream.collector import SESSION_SYMBOL, StreamCollector
from qde.stream.config import StreamConfig
from qde.stream.gaps import (
    HANDOVER,
    RECONNECT,
    SEQUENCE_JUMP,
    SESSION_START,
    SESSION_STOP,
)


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
        self._messages = list(messages)
        self._raise_exc = raise_exc
        self._i = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def recv(self):
        """The handover drains a retiring socket with recv(), not iteration."""
        if self._i < len(self._messages):
            message = self._messages[self._i]
            self._i += 1
            return message
        if self._raise_exc is not None:
            raise self._raise_exc
        raise StopAsyncIteration

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.recv()


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


# --- deliberate connection handover --------------------------------------------
#
# Binance drops a burst of messages on a connection open ~48h WITHOUT closing it
# (measured: 2,931 depth messages, six streams, same millisecond, no disconnect).
# The collector pre-empts that by replacing the connection on its own schedule —
# opening the successor first, so the overlap is de-duplicated rather than a gap.


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


def _gaps_of(tmp_path, source="binance", symbol="BTCUSDT"):
    return pd.read_parquet(
        tmp_path / f"bronze/group=microstructure/source={source}/kind=gaps/symbol={symbol}"
    )


def _trades_of(tmp_path, source="binance", symbol="BTCUSDT"):
    return pd.read_parquet(
        tmp_path / f"bronze/group=microstructure/source={source}/kind=trades/symbol={symbol}"
    )


def test_handover_loses_nothing_and_drops_the_replayed_overlap(tmp_path, monkeypatch):
    # The successor is opened while the predecessor is still live, so it replays
    # the overlap: trades 3 and 4 arrive on BOTH connections. Every trade must be
    # stored exactly once, with no hole and no duplicate.
    scripts = [
        ([_trade_msg(i) for i in (1, 2, 3, 4)], None),
        ([_trade_msg(i) for i in (3, 4, 5, 6)], None),
    ]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))
    _fake_clock(monkeypatch, step_ms=600)

    cfg = StreamConfig(
        symbols=["BTCUSDT"],
        kinds=["trades"],
        base_dir=str(tmp_path),
        flush_seconds=100,
        max_connection_seconds=2,
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=8))

    # 1..6 exactly once: the seam is invisible in the data, which is the point.
    assert list(_trades_of(tmp_path)["trade_id"]) == [1, 2, 3, 4, 5, 6]

    gaps = _gaps_of(tmp_path)
    assert list(gaps["reason"]) == [HANDOVER]
    # Nothing was missed, and the two discarded replays prove the sockets really
    # did overlap rather than the successor merely starting where the other left off.
    assert int(gaps.iloc[0]["missing_count"]) == 0
    assert int(gaps.iloc[0]["duplicates"]) == 2


def test_handover_is_not_recorded_as_an_outage(tmp_path, monkeypatch):
    # A handover and a dropped connection must stay distinguishable: if a routine
    # maintenance event logs as a reconnect, real outages lose their meaning.
    scripts = [
        ([_trade_msg(i) for i in (1, 2, 3, 4)], None),
        ([_trade_msg(i) for i in (3, 4, 5, 6)], None),
    ]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))
    _fake_clock(monkeypatch, step_ms=600)

    cfg = StreamConfig(
        symbols=["BTCUSDT"], kinds=["trades"], base_dir=str(tmp_path),
        flush_seconds=100, max_connection_seconds=2,
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=8))

    reasons = set(_gaps_of(tmp_path)["reason"])
    assert RECONNECT not in reasons
    assert SEQUENCE_JUMP not in reasons
    # Nor should it inflate the session's gap tally.
    assert collector.gap_count == 0


def test_a_handover_that_did_lose_data_is_still_caught(tmp_path, monkeypatch):
    # Sequence tracking deliberately continues across an overlapped handover, so
    # the mechanism cannot hide a real hole behind its own maintenance record.
    scripts = [
        ([_trade_msg(i) for i in (1, 2, 3, 4)], None),
        ([_trade_msg(i) for i in (90, 91, 92, 93)], None),  # a genuine jump
    ]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))
    _fake_clock(monkeypatch, step_ms=600)

    cfg = StreamConfig(
        symbols=["BTCUSDT"], kinds=["trades"], base_dir=str(tmp_path),
        flush_seconds=100, max_connection_seconds=2,
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=8))

    gaps = _gaps_of(tmp_path)
    assert SEQUENCE_JUMP in set(gaps["reason"])
    jump = gaps[gaps["reason"] == SEQUENCE_JUMP].iloc[0]
    assert int(jump["missing_count"]) == 85  # 90 follows 4


def test_a_failed_handover_keeps_the_working_connection(tmp_path, monkeypatch):
    # Losing the feed because the *replacement* could not be opened would be a
    # self-inflicted outage — strictly worse than the problem being pre-empted.
    calls = {"n": 0}

    def connect(url, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:  # the successor
            raise OSError("successor refused")
        return _FakeConn([_trade_msg(i) for i in (1, 2, 3, 4, 5, 6)], None)

    monkeypatch.setattr(collector_mod.websockets, "connect", connect)
    _fake_clock(monkeypatch, step_ms=600)

    cfg = StreamConfig(
        symbols=["BTCUSDT"], kinds=["trades"], base_dir=str(tmp_path),
        flush_seconds=100, max_connection_seconds=2,
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=6))

    # It kept reading the original socket straight through the failed attempt.
    assert list(_trades_of(tmp_path)["trade_id"]) == [1, 2, 3, 4, 5, 6]
    assert collector.gap_count == 0


def test_a_clean_close_by_the_peer_is_recorded(tmp_path, monkeypatch):
    # A server closing the socket politely raises nothing at all, so this used to
    # reconnect with no gap record whatsoever — an outage that left no trace.
    scripts = [
        ([_trade_msg(1), _trade_msg(2)], None),  # ends without raising
        ([_trade_msg(3), _trade_msg(4)], None),
    ]
    monkeypatch.setattr(collector_mod.websockets, "connect", _fake_connect(scripts))

    cfg = StreamConfig(
        symbols=["BTCUSDT"], kinds=["trades"], base_dir=str(tmp_path), flush_seconds=100,
    )
    collector = StreamCollector(cfg)
    asyncio.run(collector.run(max_messages=4))

    assert RECONNECT in set(_gaps_of(tmp_path)["reason"])


def test_unsequenced_capture_refuses_to_overlap():
    # Coinbase's l2update carries no update id, so a replayed diff is
    # indistinguishable from a new one — and replaying an old one rewinds the book.
    with_depth = StreamCollector(
        StreamConfig(source="coinbase", symbols=["BTCUSDT"], kinds=["trades", "depth"])
    )
    assert with_depth.supports_overlap is False

    # The same venue without depth is entirely sequenced, so it may overlap.
    without_depth = StreamCollector(
        StreamConfig(source="coinbase", symbols=["BTCUSDT"], kinds=["trades", "book_ticker"])
    )
    assert without_depth.supports_overlap is True

    assert StreamCollector(
        StreamConfig(symbols=["BTCUSDT"], kinds=["trades", "depth", "book_ticker"])
    ).supports_overlap is True  # binance: every stream carries an id
