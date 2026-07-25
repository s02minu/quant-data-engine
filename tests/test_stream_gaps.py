from qde.stream.gaps import RECONNECT, SEQUENCE_JUMP, SequenceTracker, reconnect_gap


def _trade(trade_id, received_at=1000):
    return {"trade_id": trade_id, "received_at": received_at}


def _depth(first, final, received_at=1000):
    return {"first_update_id": first, "final_update_id": final, "received_at": received_at}


def _book(update_id, received_at=1000):
    return {"update_id": update_id, "received_at": received_at}


def test_contiguous_trades_report_no_gap():
    t = SequenceTracker()
    assert t.check("trades", "BTCUSDT", _trade(1)) is None
    assert t.check("trades", "BTCUSDT", _trade(2)) is None


def test_trade_jump_is_counted():
    t = SequenceTracker()
    t.check("trades", "BTCUSDT", _trade(3))
    gap = t.check("trades", "BTCUSDT", _trade(7))
    assert gap["reason"] == SEQUENCE_JUMP
    assert gap["missing_count"] == 3


def test_backwards_trade_is_a_replay_not_a_gap():
    t = SequenceTracker()
    t.check("trades", "BTCUSDT", _trade(5))
    assert t.check("trades", "BTCUSDT", _trade(3)) is None


def test_depth_chain_break_is_counted():
    t = SequenceTracker()
    t.check("depth", "ETHUSDT", _depth(10, 12))
    assert t.check("depth", "ETHUSDT", _depth(13, 15)) is None
    gap = t.check("depth", "ETHUSDT", _depth(20, 22))
    assert gap["missing_count"] == 4


def test_book_ticker_checks_ordering_only():
    t = SequenceTracker()
    assert t.check("book_ticker", "SOLUSDT", _book(100)) is None
    assert t.check("book_ticker", "SOLUSDT", _book(130)) is None
    gap = t.check("book_ticker", "SOLUSDT", _book(120))
    assert gap["reason"] == SEQUENCE_JUMP
    assert gap["missing_count"] is None


def test_reset_clears_state_and_reports_tracked_streams():
    t = SequenceTracker()
    t.check("trades", "BTCUSDT", _trade(1))
    tracked = t.reset()
    assert ("trades", "BTCUSDT") in tracked
    # A fresh baseline after reset means a later id never registers as a jump.
    assert t.check("trades", "BTCUSDT", _trade(999)) is None


def test_reconnect_gap_records_the_outage_window():
    gap = reconnect_gap("trades", "BTCUSDT", disconnected_at=1000, reconnected_at=2000)
    assert gap["reason"] == RECONNECT
    assert gap["gap_start_ms"] == 1000
    assert gap["gap_end_ms"] == 2000
