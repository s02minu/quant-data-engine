import pandas as pd
import pytest

from qde.loaders.kraken_loader import load_kraken_ohlcv


def test_returns_nonempty_dataframe(offline_kraken):
    df = load_kraken_ohlcv("XBTUSD", start="2024-01-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_columns_are_correct(offline_kraken):
    df = load_kraken_ohlcv("XBTUSD", start="2024-01-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]


def test_invalid_symbol_raises(offline_kraken):
    with pytest.raises(ValueError):
        load_kraken_ohlcv("NOTREAL", start="2024-01-01")
