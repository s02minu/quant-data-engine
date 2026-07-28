from qde.stream.config import StreamConfig


def test_stream_names_use_binance_casing_at_the_boundary():
    cfg = StreamConfig(symbols=["BTCUSDT"], kinds=["trades", "depth", "book_ticker"])
    names = cfg.stream_names()

    assert "btcusdt@trade" in names
    assert "btcusdt@depth@100ms" in names
    assert "btcusdt@bookTicker" in names


def test_stream_names_respect_selected_kinds():
    cfg = StreamConfig(symbols=["ETHUSDT"], kinds=["trades"])
    assert cfg.stream_names() == ["ethusdt@trade"]


def test_each_config_gets_its_own_symbol_list():
    # pydantic copies the default list per instance, so no shared mutable default.
    a = StreamConfig()
    b = StreamConfig()
    a.symbols.append("XRPUSDT")
    assert "XRPUSDT" not in b.symbols
