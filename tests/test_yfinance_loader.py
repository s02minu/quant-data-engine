import pandas as pd
import pytest
from qde.loaders.yfinance_loader import load_yfinance_ohlcv


def test_returns_nonempty_dataframe():
    df = load_yfinance_ohlcv("BTC-USD", "2024-01-01", "2024-02-01")
    # asserts df is a DataFrame
    assert isinstance(df, pd.DataFrame)
    #asserts df is not empty
    assert not df.empty


def test_columns_are_lowercase_ohlcv():
    df = load_yfinance_ohlcv("BTC-USD", "2024-01-01", "2024-02-01")
    # asserts the column are exactly the lowercase names you expect
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.index.name == "date"


def test_invalid_symbol_raises():
    with pytest.raises(ValueError):
        load_yfinance_ohlcv("NOTAREALTICKER", "2024-01-01", "2024-02-01")