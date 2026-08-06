"""Tests for the CFTC COT ingestor. Offline via the ``offline_cftc`` fixture."""

import pandas as pd
import pytest

from qde.ingest import get_ingestor
from qde.ingest.cftc import _METRICS
from qde.loaders.exceptions import NoNewData


def _cftc():
    return get_ingestor("cftc")


def test_returns_a_wide_multi_metric_frame(offline_cftc):
    df = _cftc().load("ES", "2024-01-01")  # canonical ES -> code 13874A
    assert list(df.columns) == list(_METRICS.values())  # one column per metric
    assert df.index.name == "date"
    assert str(df.index.tz) == "UTC"
    assert len(df) == 3


def test_columns_map_to_the_right_metric(offline_cftc):
    # The canned rows give each raw column a distinct value; a scrambled mapping
    # would surface here.
    df = _cftc().load("ES", "2024-01-01")
    first = df.iloc[0]
    assert first["dealer_long"] == 100
    assert first["dealer_short"] == 110
    assert first["asset_mgr_long"] == 200
    assert first["open_interest"] == 9000


def test_missing_category_becomes_nan_row_kept(offline_cftc):
    # The third canned row omits leveraged-funds long -> NaN, but the row (and its
    # other metrics) stay.
    df = _cftc().load("ES", "2024-01-01")
    assert pd.isna(df["lev_long"].iloc[2])
    assert df["lev_short"].iloc[2] == 310  # the rest of the row survives
    assert len(df) == 3


def test_symbol_is_translated_to_the_market_code(offline_cftc, monkeypatch):
    # load("ES") must hit the API with the CFTC contract market code, not "ES".
    seen = {}
    real = _cftc()

    def spy(url, params):
        seen["where"] = params["$where"]
        from tests.conftest import FakeResponse, _cot_rows

        return FakeResponse(payload=_cot_rows())

    import qde.loaders.http as http_mod

    monkeypatch.setattr(http_mod.requests, "get", spy)
    real.load("ES", "2024-01-01")
    assert "13874A" in seen["where"] and "'ES'" not in seen["where"]


def test_caught_up_raises_nonewdata(offline_cftc):
    # A start past the last report yields an empty page -> NoNewData, the benign
    # "already up to date" case the incremental update relies on.
    with pytest.raises(NoNewData):
        _cftc().load("ES", "2025-01-01")
