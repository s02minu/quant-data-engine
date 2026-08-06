"""Tests for the registry-driven data-quality checks (freshness + nulls)."""

import numpy as np
import pandas as pd

from qde.checks import run_checks
from qde.storage import upsert_series, upsert_series_frame

NOW = pd.Timestamp("2026-08-06", tz="UTC")


def _daily(last: str, n: int, value: float = 20.0):
    """A daily series of ``n`` points ending on ``last`` (UTC)."""
    idx = pd.date_range(end=last, periods=n, freq="D", tz="UTC")
    return pd.DataFrame({"value": [value] * n}, index=idx.rename("date"))


def test_fresh_series_passes(tmp_path):
    # A daily CBOE series current as of NOW is not flagged.
    upsert_series(_daily("2026-08-05", 30), "VIX", "cboe", base_dir=str(tmp_path))
    assert run_checks(str(tmp_path), now=NOW) == []


def test_stale_series_is_flagged(tmp_path):
    # A daily series whose last point is 30 days back trips the freshness check.
    upsert_series(_daily("2026-07-07", 30), "VIX", "cboe", base_dir=str(tmp_path))
    v = run_checks(str(tmp_path), now=NOW)
    assert len(v) == 1
    assert v[0].check == "freshness" and v[0].label() == "cboe/VIX"


def test_period_lagged_monthly_not_flagged(tmp_path):
    # FRED monthly series are dated by period start but published with a lag, so a
    # last observation ~2 months old is *current*, not stale. Regression guard for
    # the false positive the 3x factor fixes.
    idx = pd.date_range(end="2026-06-01", periods=24, freq="MS", tz="UTC")
    df = pd.DataFrame({"value": np.arange(24.0)}, index=idx.rename("date"))
    upsert_series(df, "CPIAUCSL", "fred", base_dir=str(tmp_path))
    assert run_checks(str(tmp_path), now=NOW) == []


def test_null_tolerance_breach_is_flagged(tmp_path):
    # CBOE tolerates zero nulls in `value`; a single NaN is an error-level violation.
    df = _daily("2026-08-05", 10)
    df.iloc[-1, 0] = np.nan
    upsert_series(df, "VIX", "cboe", base_dir=str(tmp_path))
    v = run_checks(str(tmp_path), now=NOW)
    assert [x.check for x in v] == ["nulls"]
    assert v[0].severity == "error" and v[0].source == "cboe"


def test_fred_tolerates_nulls(tmp_path):
    # FRED declares full null tolerance (a "." is "not yet published", not a defect).
    df = _daily("2026-08-05", 10)
    df.iloc[0, 0] = np.nan
    upsert_series(df, "DGS10", "fred", base_dir=str(tmp_path))
    assert run_checks(str(tmp_path), now=NOW) == []


def test_multi_metric_freshness_flagged_once(tmp_path):
    # A stale multi-metric market yields ONE freshness violation, not one per metric
    # (the metrics share report dates).
    idx = pd.date_range(end="2026-05-01", periods=10, freq="W", tz="UTC").rename("date")
    wide = pd.DataFrame({"dealer_long": range(10), "dealer_short": range(10)}, index=idx)
    upsert_series_frame(wide, "ES", "cftc", base_dir=str(tmp_path))
    v = run_checks(str(tmp_path), now=NOW)
    fresh = [x for x in v if x.check == "freshness"]
    assert len(fresh) == 1 and fresh[0].label() == "cftc/ES"
