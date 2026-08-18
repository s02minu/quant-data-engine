"""Tests for the `series` group storage (scalar time series)."""

import pandas as pd
import pytest

from qde.storage import (
    _series_path,
    list_series,
    query,
    series_watermark,
    upsert_series,
    upsert_series_frame,
)


def _series(dates, values):
    idx = pd.DatetimeIndex(pd.to_datetime(list(dates), utc=True), name="date")
    return pd.DataFrame({"value": list(values)}, index=idx)


def _wide(dates, **metrics):
    idx = pd.DatetimeIndex(pd.to_datetime(list(dates), utc=True), name="date")
    return pd.DataFrame({m: list(v) for m, v in metrics.items()}, index=idx)


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


def test_upsert_series_frame_single_value_writes_flat(tmp_path):
    # A one-column 'value' frame (FRED/CBOE) is stored flat, no metric partition.
    base = str(tmp_path)
    flat = _series(["2024-01-01", "2024-01-02"], [1.0, 2.0])
    n = upsert_series_frame(flat, "DGS10", "fred", base)
    assert n == 2
    assert _series_path("DGS10", "fred", base).exists()  # flat file, no metric=


def test_upsert_series_frame_splits_wide_into_metrics(tmp_path):
    # A wide frame (COT shape) writes one file per column under a metric partition.
    base = str(tmp_path)
    df = _wide(
        ["2024-01-02", "2024-01-09"],
        dealer_long=[100, 110],
        dealer_short=[200, 210],
        open_interest=[9000, 9100],
    )
    total = upsert_series_frame(df, "ES", "cftc", base)
    assert total == 6  # 3 metrics x 2 dates
    for m in ("dealer_long", "dealer_short", "open_interest"):
        assert _series_path("ES", "cftc", base, metric=m).exists()

    out = query(
        "SELECT metric, value FROM series WHERE series_id='ES' AND date='2024-01-09' "
        "ORDER BY metric",
        base_dir=base,
    )
    assert list(zip(out["metric"], out["value"], strict=True)) == [
        ("dealer_long", 110.0),
        ("dealer_short", 210.0),
        ("open_interest", 9100.0),
    ]


def test_series_watermark_spans_metric_partitions(tmp_path):
    # A multi-metric series has no flat file; its watermark is the max across the
    # metric partitions (all metrics of a market share the same report dates).
    base = str(tmp_path)
    df = _wide(
        ["2024-01-02", "2024-01-09"], dealer_long=[100, 110], open_interest=[9000, 9100]
    )
    upsert_series_frame(df, "ES", "cftc", base)
    assert series_watermark("ES", "cftc", base) == pd.Timestamp("2024-01-09", tz="UTC")


def test_query_unions_flat_and_metric_series(tmp_path):
    # The mixed-depth case: a flat FRED series and a multi-metric COT series in the
    # SAME lake must both surface through one `series` view (metric NULL on the flat
    # side). A single glob over both depths raises "Hive partition mismatch".
    base = str(tmp_path)
    upsert_series(_series(["2024-01-01"], [4.0]), "DGS10", "fred", base_dir=base)
    upsert_series_frame(
        _wide(["2024-01-02"], dealer_long=[100], open_interest=[9000]), "ES", "cftc", base
    )

    out = query("SELECT source, series_id, metric, value FROM series", base_dir=base)
    got = {(r.source, r.series_id, r.metric, r.value) for r in out.itertuples(index=False)}
    assert ("cftc", "ES", "dealer_long", 100.0) in got
    assert ("cftc", "ES", "open_interest", 9000.0) in got
    fred = next(r for r in out.itertuples(index=False) if r.source == "fred")
    assert fred.value == 4.0 and pd.isna(fred.metric)  # flat side: metric filled NULL


# --- reading a stored series back as the wide frame an ingestor returns -----------


def test_a_single_value_series_round_trips(tmp_path):
    from qde.storage import load_series_local, upsert_series

    idx = pd.DatetimeIndex(pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True), name="date")
    upsert_series(pd.DataFrame({"value": [1.0, 2.0]}, index=idx), "UNRATE", "fred", str(tmp_path))

    back = load_series_local("UNRATE", "fred", str(tmp_path))
    assert list(back.columns) == ["value"]
    assert back["value"].tolist() == [1.0, 2.0]


def test_a_multi_metric_series_is_reassembled_wide(tmp_path):
    # The exact inverse of upsert_series_frame: it splits a wide frame into one file
    # per metric, so a comparison against a fresh fetch has to put it back together.
    from qde.storage import load_series_local, upsert_series_frame

    idx = pd.DatetimeIndex(pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True), name="date")
    wide = pd.DataFrame({"dealer_long": [10.0, 11.0], "dealer_short": [5.0, 6.0]}, index=idx)
    upsert_series_frame(wide, "VIX", "cftc", str(tmp_path))

    back = load_series_local("VIX", "cftc", str(tmp_path))
    assert sorted(back.columns) == ["dealer_long", "dealer_short"]
    assert back["dealer_short"].tolist() == [5.0, 6.0]


def test_a_metric_that_starts_later_does_not_truncate_the_others(tmp_path):
    # An inner join would silently shorten every other metric to the newest one's
    # history — losing years of data to a column added last month.
    from qde.storage import load_series_local, upsert_series

    long_idx = pd.DatetimeIndex(
        pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"], utc=True), name="date"
    )
    short_idx = pd.DatetimeIndex(pd.to_datetime(["2024-01-03"], utc=True), name="date")
    upsert_series(pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=long_idx),
                  "VIX", "cftc", str(tmp_path), metric="old")
    upsert_series(pd.DataFrame({"value": [9.0]}, index=short_idx),
                  "VIX", "cftc", str(tmp_path), metric="new")

    back = load_series_local("VIX", "cftc", str(tmp_path))
    assert len(back) == 3, "the longer metric must keep its full history"
    assert back["old"].tolist() == [1.0, 2.0, 3.0]


def test_an_unstored_series_raises(tmp_path):
    from qde.storage import load_series_local

    with pytest.raises(FileNotFoundError):
        load_series_local("NOPE", "fred", str(tmp_path))
