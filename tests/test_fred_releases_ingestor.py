"""Tests for the FRED/ALFRED events (release-calendar) ingestor.

Offline via the ``offline_fred_releases`` fixture (canned all-vintages payload).
The ingestor's job is to fold the ``(reference period, vintage)`` grid into the
bitemporal events schema, so these assert that folding: one event per reference
period, one row per revision, the two clocks, and the derived columns.
"""

import pandas as pd
import pytest

from qde.ingest import get_ingestor


def _events():
    return get_ingestor("fredcal").load_native("CPIAUCSL", "2024-01-01")


def test_returns_the_events_schema(offline_fred_releases):
    df = _events()
    assert list(df.columns) == [
        "event_id",
        "series_id",
        "scheduled_ts",
        "observed_ts",
        "actual",
        "forecast",
        "previous",
        "revision_seq",
    ]
    assert str(df["scheduled_ts"].dt.tz) == "UTC"
    assert str(df["observed_ts"].dt.tz) == "UTC"
    assert len(df) == 5  # 2 + 2 + 1 vintages across three reference months


def test_one_event_per_reference_period_revisions_counted(offline_fred_releases):
    df = _events().set_index(["event_id", "revision_seq"])
    # Month 01: initial then one revision.
    assert df.loc[("CPIAUCSL:2024-01-01", 0), "actual"] == 3.1
    assert df.loc[("CPIAUCSL:2024-01-01", 1), "actual"] == 3.2
    # revision_seq is contiguous from 0 within each event.
    seqs = _events().groupby("event_id")["revision_seq"].apply(lambda s: sorted(s))
    assert seqs["CPIAUCSL:2024-01-01"] == [0, 1]
    assert seqs["CPIAUCSL:2024-02-01"] == [0, 1]
    assert seqs["CPIAUCSL:2024-03-01"] == [0]


def test_scheduled_ts_is_the_first_vintage_constant_per_event(offline_fred_releases):
    # scheduled_ts is the initial-print date and is the same for every revision.
    df = _events()
    m1 = df[df["event_id"] == "CPIAUCSL:2024-01-01"]
    assert (m1["scheduled_ts"] == pd.Timestamp("2024-02-15", tz="UTC")).all()
    m2 = df[df["event_id"] == "CPIAUCSL:2024-02-01"]
    assert (m2["scheduled_ts"] == pd.Timestamp("2024-03-15", tz="UTC")).all()


def test_observed_ts_is_the_vintage_date(offline_fred_releases):
    df = _events().set_index(["event_id", "revision_seq"])
    assert df.loc[("CPIAUCSL:2024-01-01", 0), "observed_ts"] == pd.Timestamp(
        "2024-02-15", tz="UTC"
    )
    assert df.loc[("CPIAUCSL:2024-01-01", 1), "observed_ts"] == pd.Timestamp(
        "2024-03-15", tz="UTC"
    )


def test_bitemporal_ordering_holds(offline_fred_releases):
    # observed_ts >= scheduled_ts for every row — the invariant the DQ check guards.
    df = _events()
    assert (df["observed_ts"] >= df["scheduled_ts"]).all()


def test_previous_is_prior_periods_initial_print(offline_fred_releases):
    df = _events().drop_duplicates("event_id").set_index("event_id")
    assert pd.isna(df.loc["CPIAUCSL:2024-01-01", "previous"])  # first period, no prior
    assert df.loc["CPIAUCSL:2024-02-01", "previous"] == 3.1  # month-01 initial
    assert df.loc["CPIAUCSL:2024-03-01", "previous"] == 3.5  # month-02 initial


def test_withheld_value_becomes_nan_row_kept(offline_fred_releases):
    df = _events().set_index(["event_id", "revision_seq"])
    # Month 03's initial is a withheld ".", coerced to NaN but the event kept.
    assert pd.isna(df.loc[("CPIAUCSL:2024-03-01", 0), "actual"])


def test_forecast_is_always_nan_code_only_column(offline_fred_releases):
    df = _events()
    assert df["forecast"].isna().all()


def test_bad_series_raises(offline_fred_releases):
    with pytest.raises(ValueError):
        get_ingestor("fredcal").load_native("NOTREAL", "2024-01-01")


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="FRED_API_KEY"):
        get_ingestor("fredcal").load_native("CPIAUCSL", "2024-01-01")
