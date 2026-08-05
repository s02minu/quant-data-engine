import pandas as pd
import pytest

from qde.ingest import get_ingestor


def _binance():
    return get_ingestor("binance")


def test_returns_nonempty_dataframe(offline_binance):
    df = _binance().load_native("BTCUSDT", "2024-01-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_columns_are_lowercase_ohlcv(offline_binance):
    df = _binance().load_native("BTCUSDT", "2024-01-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.index.name == "date"


def test_invalid_symbol_raises(offline_binance):
    with pytest.raises(ValueError):
        _binance().load_native("NOTAREALTICKER", "2024-01-01")
