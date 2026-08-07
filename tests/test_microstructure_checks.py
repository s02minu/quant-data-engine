"""Tests for the streamed-microstructure data-quality checks."""

import datetime as dt

import pandas as pd

from qde.checks import run_microstructure_checks

DAY = dt.date(2026, 8, 6)  # a settled day
NOW = pd.Timestamp("2026-08-07 00:30", tz="UTC")  # nightly time; DAY is "yesterday"


def _write(tmp_path, source, kind, symbol, rows, day=DAY):
    """Write one bronze microstructure part file for (source, kind, symbol, day)."""
    part = (
        tmp_path
        / "bronze"
        / "group=microstructure"
        / f"source={source}"
        / f"kind={kind}"
        / f"symbol={symbol}"
        / f"date={day.isoformat()}"
        / "part-000.parquet"
    )
    part.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(part, engine="pyarrow", index=False)


def _quote(bid, ask, bid_qty="1.0", ask_qty="1.0", symbol="BTCUSDT"):
    return {
        "symbol": symbol,
        "bid_price": bid,
        "bid_qty": bid_qty,
        "ask_price": ask,
        "ask_qty": ask_qty,
        "update_id": 1,
        "received_at": 1,
    }


def _trade(symbol="BTCUSDT"):
    return {"symbol": symbol, "trade_id": 1, "price": "100.0", "quantity": "1.0", "received_at": 1}


def _healthy(tmp_path, source, symbol="BTCUSDT"):
    """A well-behaved feed: trades + a sane top-of-book (bid < ask)."""
    _write(tmp_path, source, "trades", symbol, [_trade(symbol)])
    _write(tmp_path, source, "book_ticker", symbol, [_quote("99.99", "100.01", symbol=symbol)])


def test_no_microstructure_yields_nothing(tmp_path):
    assert run_microstructure_checks(str(tmp_path), day=DAY) == []


def test_healthy_feed_passes(tmp_path):
    _healthy(tmp_path, "binance")
    _healthy(tmp_path, "coinbase")
    assert run_microstructure_checks(str(tmp_path), day=DAY) == []


def test_missing_trades_flags_activity(tmp_path):
    # book_ticker present but no trade tape -> a partial-feed warning.
    _write(tmp_path, "coinbase", "book_ticker", "BTCUSDT", [_quote("99.99", "100.01")])
    v = run_microstructure_checks(str(tmp_path), day=DAY)
    assert [(x.check, x.severity, x.metric) for x in v] == [("activity", "warn", "trades")]
    assert x_label(v, "activity") == "coinbase/BTCUSDT/trades"


def test_crossed_book_is_an_error(tmp_path):
    _write(tmp_path, "binance", "trades", "BTCUSDT", [_trade()])
    # Two crossed quotes (bid > ask) among three.
    _write(
        tmp_path,
        "binance",
        "book_ticker",
        "BTCUSDT",
        [_quote("100.05", "100.00"), _quote("100.06", "100.00"), _quote("99.99", "100.01")],
    )
    v = run_microstructure_checks(str(tmp_path), day=DAY)
    crossed = [x for x in v if x.check == "crossed_book"]
    assert len(crossed) == 1
    assert crossed[0].severity == "error"
    assert crossed[0].label() == "binance/BTCUSDT/book_ticker"
    assert "2 crossed" in crossed[0].detail and "of 3" in crossed[0].detail


def test_negative_size_is_an_error(tmp_path):
    _write(tmp_path, "binance", "trades", "BTCUSDT", [_trade()])
    _write(
        tmp_path, "binance", "book_ticker", "BTCUSDT", [_quote("99.99", "100.01", bid_qty="-1.0")]
    )
    v = run_microstructure_checks(str(tmp_path), day=DAY)
    crossed = [x for x in v if x.check == "crossed_book"]
    assert len(crossed) == 1 and "negative size" in crossed[0].detail


def test_sequence_jump_gap_is_an_error(tmp_path):
    _healthy(tmp_path, "binance")
    _write(
        tmp_path,
        "binance",
        "gaps",
        "BTCUSDT",
        [
            {
                "stream_kind": "trades",
                "symbol": "BTCUSDT",
                "reason": "sequence_jump",
                "missing_count": 7,
            }
        ],
    )
    v = run_microstructure_checks(str(tmp_path), day=DAY)
    gaps = [x for x in v if x.check == "gaps"]
    assert len(gaps) == 1
    assert gaps[0].severity == "error"
    assert "1 sequence-jump" in gaps[0].detail and "7 msgs" in gaps[0].detail


def test_reconnect_only_gap_is_a_warning(tmp_path):
    _healthy(tmp_path, "coinbase")
    _write(
        tmp_path,
        "coinbase",
        "gaps",
        "BTCUSDT",
        [
            {
                "stream_kind": "trades",
                "symbol": "BTCUSDT",
                "reason": "reconnect",
                "missing_count": None,
            }
        ],
    )
    v = run_microstructure_checks(str(tmp_path), day=DAY)
    gaps = [x for x in v if x.check == "gaps"]
    assert len(gaps) == 1 and gaps[0].severity == "warn"
    assert "1 reconnect" in gaps[0].detail


def test_only_the_target_day_is_checked(tmp_path):
    # A crossed book on a *different* day must not surface for DAY.
    _healthy(tmp_path, "binance")
    _write(
        tmp_path,
        "binance",
        "book_ticker",
        "BTCUSDT",
        [_quote("100.05", "100.00")],
        day=dt.date(2026, 8, 5),
    )
    assert run_microstructure_checks(str(tmp_path), day=DAY) == []


def test_day_defaults_to_yesterday(tmp_path):
    # With now=NOW (08-07 00:30), the checked day is 08-06 (DAY).
    _write(tmp_path, "binance", "book_ticker", "BTCUSDT", [_quote("100.05", "100.00")])
    v = run_microstructure_checks(str(tmp_path), now=NOW)
    assert any(x.check == "crossed_book" for x in v)


def x_label(violations, check):
    return next(x.label() for x in violations if x.check == check)
