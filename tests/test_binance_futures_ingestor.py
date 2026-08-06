"""Tests for the Binance perp funding ingestor. Offline via ``offline_binance_futures``."""

import pandas as pd
import pytest

from qde.ingest import get_ingestor
from qde.ingest.binance_futures import BinanceFuturesIngestor
from qde.loaders.exceptions import NoNewData
from qde.registry.spec import SourceSpec


def _binancefut():
    return get_ingestor("binancefut")


def test_returns_a_wide_funding_frame(offline_binance_futures):
    df = _binancefut().load("BTCUSDT", "2024-01-01")
    assert list(df.columns) == ["funding_rate", "mark_price"]  # multi-metric
    assert df.index.name == "date"
    assert str(df.index.tz) == "UTC"
    assert len(df) == 3
    # 8-hourly settlements come back as distinct intraday timestamps.
    assert [t.hour for t in df.index] == [0, 8, 16]


def test_values_and_missing_markprice(offline_binance_futures):
    df = _binancefut().load("BTCUSDT", "2024-01-01")
    assert list(df["funding_rate"]) == [0.0001, 0.0002, -0.0003]
    assert pd.isna(df["mark_price"].iloc[0])  # empty markPrice -> NaN, row kept
    assert df["mark_price"].iloc[1] == 42000.0


def test_paginates_by_funding_time(offline_binance_futures):
    # A small page cap forces the cursor walk: page one hits the limit, so the
    # loop must advance past the last settlement and fetch the remainder.
    spec = SourceSpec(
        group="series",
        name="binancefut",
        symbols={"BTCUSDT": "BTCUSDT"},
        max_rows_per_call=2,
    )
    df = BinanceFuturesIngestor(spec).load_native("BTCUSDT", "2024-01-01")
    assert len(df) == 3  # 2 on the first page + 1 on the second, no dupes


def test_caught_up_raises_nonewdata(offline_binance_futures):
    # A start past the last settlement yields an empty page -> NoNewData.
    with pytest.raises(NoNewData):
        _binancefut().load("BTCUSDT", "2025-01-01")
