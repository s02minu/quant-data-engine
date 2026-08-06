"""Tests for the shared ccxt bars ingestor. Offline via the ``offline_ccxt`` fixture."""

import pytest

from qde.ingest import get_ingestor
from qde.ingest.ccxt_bars import CcxtIngestor
from qde.loaders.exceptions import NoNewData
from qde.registry.spec import SourceSpec


def _okx():
    return get_ingestor("okx")  # a ccxt-backed source


def test_returns_the_bars_shape(offline_ccxt):
    df = _okx().load("BTCUSDT", "2024-01-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.index.name == "date"
    assert str(df.index.tz) == "UTC"
    assert len(df) == 3
    assert df["close"].iloc[1] == 44900.0  # values carried through, not scrambled


def test_symbol_is_translated_to_the_venue_symbol(offline_ccxt):
    # okx maps canonical BTCUSDT -> the ccxt unified 'BTC/USDT'.
    _okx().load("BTCUSDT", "2024-01-01")
    assert offline_ccxt.last_symbol == "BTC/USDT"


def test_coinbase_uses_the_usd_pair(offline_ccxt):
    # Coinbase lists BTC/USD, not USDT — the per-source map handles it.
    get_ingestor("coinbase").load("BTCUSDT", "2024-01-01")
    assert offline_ccxt.last_symbol == "BTC/USD"


def test_paginates_forward_by_time(offline_ccxt):
    # A page cap below the data size forces the cursor walk: the loop must advance
    # past the last candle and fetch the rest, terminating on the empty page.
    spec = SourceSpec(
        group="bars", name="okx", symbols={"BTCUSDT": "BTC/USDT"}, max_rows_per_call=2
    )
    df = CcxtIngestor(spec).load_native("BTC/USDT", "2024-01-01")
    assert len(df) == 3  # 2 on the first page + 1 on the second, deduped


def test_end_filters_the_range(offline_ccxt):
    df = _okx().load("BTCUSDT", "2024-01-01", end="2024-01-02")
    assert [str(d.date()) for d in df.index] == ["2024-01-01", "2024-01-02"]


def test_caught_up_raises_nonewdata(offline_ccxt):
    # A start past the last candle yields an empty first page -> NoNewData.
    with pytest.raises(NoNewData):
        _okx().load("BTCUSDT", "2030-01-01")
