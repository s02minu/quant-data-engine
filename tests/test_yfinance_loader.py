import pandas as pd
import pytest

from qde.ingest import get_ingestor


def _yfinance():
    return get_ingestor("yfinance")


def test_returns_nonempty_dataframe(offline_yfinance):
    df = _yfinance().load_native("BTC-USD", "2024-01-01", "2024-02-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_columns_are_lowercase_ohlcv(offline_yfinance):
    df = _yfinance().load_native("BTC-USD", "2024-01-01", "2024-02-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.index.name == "date"


def test_invalid_symbol_raises(offline_yfinance):
    with pytest.raises(ValueError):
        _yfinance().load_native("NOTAREALTICKER", "2024-01-01", "2024-02-01")
