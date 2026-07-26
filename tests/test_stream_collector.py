import asyncio
import json

import pandas as pd

import qde.stream.collector as collector_mod
from qde.stream.collector import SESSION_SYMBOL, StreamCollector
from qde.stream.config import StreamConfig
from qde.stream.gaps import RECONNECT, SESSION_START, SESSION_STOP


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

    def connect(url):
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
