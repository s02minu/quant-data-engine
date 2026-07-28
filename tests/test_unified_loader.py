import pandas as pd
import pytest

from qde.loaders import load_ohlcv


def test_load_via_yfinance(offline_yfinance):
    df = load_ohlcv("BTCUSDT", start="2019-01-01", end="2019-02-01", source="yfinance")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_load_via_binance(offline_binance):
    df = load_ohlcv("BTCUSDT", start="2019-01-01", end="2019-02-01", source="binance")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


# Bad source and unmapped symbol both raise on the symbol-map lookup, before any
# network call — so these need no fixture.
def test_bad_source():
    with pytest.raises(ValueError):
        load_ohlcv("BTCUSDT", start="2019-01-01", end="2019-02-01", source="bloomberg")


def test_inexistent_symbol():
    with pytest.raises(ValueError):
        load_ohlcv("SPY", start="2019-01-01", end="2019-02-01", source="binance")
