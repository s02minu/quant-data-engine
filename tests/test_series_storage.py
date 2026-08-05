"""Tests for the `series` group storage (scalar time series)."""

import pandas as pd

from qde.storage import (
    _series_path,
    list_series,
    query,
    series_watermark,
    upsert_series,
)


def _series(dates, values):
    idx = pd.DatetimeIndex(pd.to_datetime(list(dates), utc=True), name="date")
    return pd.DataFrame({"value": list(values)}, index=idx)


def test_upsert_roundtrips_and_counts(tmp_path):
    df = _series(["2024-01-01", "2024-02-01", "2024-03-01"], [3.4, 3.1, 3.5])
    n = upsert_series(df, "CPIAUCSL", "fred", base_dir=str(tmp_path))
    assert n == 3
    assert _series_path("CPIAUCSL", "fred", str(tmp_path)).exists()


def test_upsert_is_idempotent_last_write_wins(tmp_path):
    base = str(tmp_path)
    upsert_series(_series(["2024-01-01", "2024-02-01"], [1.0, 2.0]), "DGS10", "fred", base_dir=base)
    # Overlapping re-pull: 2024-02-01 is superseded, 2024-03-01 is new.
    rows = upsert_series(
        _series(["2024-02-01", "2024-03-01"], [9.9, 3.0]), "DGS10", "fred", base_dir=base
    )
    assert rows == 3  # one row per date, not accumulated

    out = query(
        "SELECT date, value FROM series WHERE series_id = 'DGS10' ORDER BY date", base_dir=base
    )
    assert list(out["value"]) == [1.0, 9.9, 3.0]  # last write won on 2024-02-01


def test_watermark_is_the_latest_date(tmp_path):
    base = str(tmp_path)
    assert series_watermark("UNRATE", "fred", base) is None  # absent
    upsert_series(
        _series(["2024-01-01", "2024-02-01"], [3.7, 3.9]), "UNRATE", "fred", base_dir=base
    )
    assert series_watermark("UNRATE", "fred", base) == pd.Timestamp("2024-02-01", tz="UTC")


def test_list_series_discovers_from_partitions(tmp_path):
    base = str(tmp_path)
    assert list_series(base).empty  # nothing seeded yet
    upsert_series(_series(["2024-01-01"], [1.0]), "DGS10", "fred", base_dir=base)
    upsert_series(_series(["2024-01-01"], [2.0]), "VIX", "cboe", base_dir=base)

    listed = list_series(base)
    got = {(r.source, r.series_id) for r in listed.itertuples(index=False)}
    assert got == {("fred", "DGS10"), ("cboe", "VIX")}


def test_metric_partition_separates_scalars(tmp_path):
    base = str(tmp_path)
    # One symbol, two metrics -> two files under a metric partition.
    upsert_series(
        _series(["2024-01-01"], [0.01]), "BTCUSDT", "binance", base_dir=base, metric="funding_rate"
    )
    upsert_series(
        _series(["2024-01-01"], [1234.0]),
        "BTCUSDT",
        "binance",
        base_dir=base,
        metric="open_interest",
    )
    assert _series_path("BTCUSDT", "binance", base, metric="funding_rate").exists()
    assert _series_path("BTCUSDT", "binance", base, metric="open_interest").exists()

    out = query(
        "SELECT metric, value FROM series WHERE series_id = 'BTCUSDT' ORDER BY metric",
        base_dir=base,
    )
    assert list(out["metric"]) == ["funding_rate", "open_interest"]
