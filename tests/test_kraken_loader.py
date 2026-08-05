import pandas as pd
import pytest

from qde.ingest import get_ingestor


def _kraken():
    return get_ingestor("kraken")


def test_returns_nonempty_dataframe(offline_kraken):
    df = _kraken().load_native("XBTUSD", start="2024-01-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_columns_are_correct(offline_kraken):
    df = _kraken().load_native("XBTUSD", start="2024-01-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]


def test_invalid_symbol_raises(offline_kraken):
    with pytest.raises(ValueError):
        _kraken().load_native("NOTREAL", start="2024-01-01")
