import pytest

from qde.stream.parsers import (
    now_ms,
    parse_book_ticker,
    parse_message,
    parse_trade,
)


def test_parse_trade_preserves_price_strings_and_stamps_received_at():
    data = {
        "s": "BTCUSDT",
        "t": 42,
        "p": "64996.01000000",
        "q": "0.5",
        "T": 111,
        "E": 112,
        "m": True,
    }
    row = parse_trade(data, received_at=999)

    # Prices stay as exact strings; no float conversion at the bronze boundary.
    assert row["price"] == "64996.01000000"
    assert isinstance(row["price"], str)
    assert row["trade_id"] == 42
    assert row["received_at"] == 999


def test_parse_message_routes_by_stream_name():
    trade = {"s": "BTCUSDT", "t": 1, "p": "1", "q": "1", "T": 1, "E": 1, "m": False}
    depth = {"s": "BTCUSDT", "U": 1, "u": 2, "E": 1, "b": [], "a": []}
    book = {"s": "BTCUSDT", "u": 1, "b": "1", "B": "1", "a": "1", "A": "1"}

    assert parse_message("btcusdt@trade", trade, 1)[0] == "trades"
    assert parse_message("btcusdt@depth@100ms", depth, 1)[0] == "depth"
    assert parse_message("btcusdt@bookTicker", book, 1)[0] == "book_ticker"


def test_parse_message_raises_on_unknown_stream():
    with pytest.raises(ValueError):
        parse_message("btcusdt@kline_1m", {}, 1)


def test_book_ticker_has_no_event_time():
    book = {"s": "BTCUSDT", "u": 7, "b": "1", "B": "1", "a": "1", "A": "1"}
    row = parse_book_ticker(book, received_at=5)

    assert "event_time" not in row
    assert row["received_at"] == 5


def test_now_ms_returns_epoch_milliseconds():
    value = now_ms()
    assert isinstance(value, int)
    assert value > 1_000_000_000_000
