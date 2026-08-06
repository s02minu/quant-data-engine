"""Tests for the CBOE volatility-index ingestor. Offline via ``offline_cboe``."""

import pytest

from qde.ingest import get_ingestor
from qde.loaders.exceptions import NoNewData


def _cboe():
    return get_ingestor("cboe")


def test_returns_the_series_shape(offline_cboe):
    df = _cboe().load_native("VIX", "2024-01-01")
    assert list(df.columns) == ["value"]
    assert df.index.name == "date"
    assert str(df.index.tz) == "UTC"
    assert len(df) == 3


def test_vix_value_is_close_not_open(offline_cboe):
    # VIX carries OPEN/HIGH/LOW/CLOSE; the series value must be CLOSE (the last
    # column), never OPEN — the canned CSV makes them differ so this is provable.
    df = _cboe().load_native("VIX", "2024-01-01")
    assert list(df["value"]) == [12.5, 13.75, 14.9]


def test_single_column_indices_parse(offline_cboe):
    # VVIX/SKEW have a single value column; the same "last column" rule applies.
    assert list(_cboe().load_native("VVIX", "2024-01-01")["value"]) == [80.0, 82.5, 85.1]
    assert list(_cboe().load_native("SKEW", "2024-01-01")["value"]) == [120.0, 121.5, 119.9]


def test_start_filters_client_side(offline_cboe):
    # The CDN serves the whole history with no date param, so the ingestor must
    # narrow to [start, end] itself. Starting mid-history drops the earlier rows.
    df = _cboe().load_native("VIX", "2024-01-03")
    assert [str(d.date()) for d in df.index] == ["2024-01-03", "2024-01-04"]


def test_end_filters_client_side(offline_cboe):
    df = _cboe().load_native("VIX", "2024-01-01", end="2024-01-02")
    assert [str(d.date()) for d in df.index] == ["2024-01-02"]


def test_caught_up_raises_nonewdata(offline_cboe):
    # A start past the last stored date yields an empty slice -> NoNewData, the
    # benign "already up to date" case the incremental update relies on.
    with pytest.raises(NoNewData):
        _cboe().load_native("VIX", "2024-02-01")


def test_bad_series_raises(offline_cboe):
    # The CDN 404s an unknown index; get_with_requests turns that into ValueError.
    with pytest.raises(ValueError):
        _cboe().load_native("NOTREAL", "2024-01-01")
