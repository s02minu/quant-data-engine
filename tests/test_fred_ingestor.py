"""Tests for the FRED series ingestor. Offline via the ``offline_fred`` fixture."""

import pandas as pd
import pytest

from qde.ingest import get_ingestor


def _fred():
    return get_ingestor("fred")


def test_returns_the_series_shape(offline_fred):
    df = _fred().load_native("CPIAUCSL", "2024-01-01")
    assert list(df.columns) == ["value"]
    assert df.index.name == "date"
    assert str(df.index.tz) == "UTC"
    assert len(df) == 3


def test_missing_value_becomes_nan_row_kept(offline_fred):
    df = _fred().load_native("CPIAUCSL", "2024-01-01")
    assert df["value"].iloc[0] == 3.1
    assert df["value"].iloc[1] == 3.2
    assert pd.isna(df["value"].iloc[2])  # FRED "." -> NaN, the row is not dropped


def test_bad_series_raises(offline_fred):
    with pytest.raises(ValueError):
        _fred().load_native("NOTREAL", "2024-01-01")


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="FRED_API_KEY"):
        _fred().load_native("CPIAUCSL", "2024-01-01")
