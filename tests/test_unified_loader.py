import pandas as pd
import pytest

from qde.loaders import  load_ohlcv

# Test Loading via yfinance
def test_load_via_yfinance():
    df = load_ohlcv("BTCUSDT", start="2019-01-01", end="2019-02-01", source="yfinance")
    assert isinstance(df, pd.DataFrame)
    # asserts df is not empty
    assert not df.empty


# Test Loading via yfinance
def test_load_via_binance():
    df = load_ohlcv("BTCUSDT", start="2019-01-01", end="2019-02-01", source="binance")
    assert isinstance(df, pd.DataFrame)
    # asserts df is not empty
    assert not df.empty


# Test Loading via bad source
def test_bad_source():
    with pytest.raises(ValueError):
        load_ohlcv("BTCUSDT", start="2019-01-01", end="2019-02-01", source="bloomberg")


# Test for symbol that doesn't exit in source
def test_inexistent_symbol():
    with pytest.raises(ValueError):
        load_ohlcv("SPY", start="2019-01-01", end="2019-02-01", source="binance")







