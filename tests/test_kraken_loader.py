import pandas as pd
import pytest
from qde.loaders.kraken_loader import load_kraken_ohlcv


def test_returns_nonempty_dataframe():
    df = load_kraken_ohlcv("XBTUSD", start="2024-01-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_columns_are_correct():
    df = load_kraken_ohlcv("XBTUSD", start="2024-01-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]


def test_invalid_symbol_raises():
    with pytest.raises(ValueError):
        load_kraken_ohlcv("NOTREAL", start="2024-01-01")