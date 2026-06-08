import pandas as pd
import pytest

from qde.loaders.binance_loader import load_binance_ohlcv

# Test to confirm returned object is a DataFrame
def test_returns_nonempty_dataframe():
    df = load_binance_ohlcv("BTCUSDT", "2024-01-01")
    # asserts df is a DataFrame
    assert isinstance(df, pd.DataFrame)
    #asserts df is not empty
    assert not df.empty

# Test to make sure columns are lowercase
def test_columns_are_lowercase_ohlcv():
    df = load_binance_ohlcv("BTCUSDT", "2024-01-01")
    # asserts the column are exactly the lowercase names you expect
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.index.name == "date"


# Test to see response when invalid symbol is requested.
def test_invalid_symbol_raises():
    with pytest.raises(ValueError):
        load_binance_ohlcv("NOTAREALTICKER", "2024-01-01")