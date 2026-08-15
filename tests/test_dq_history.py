"""Tests for the append-only data-quality history."""

import pandas as pd

from qde.checks import Violation
from qde.dq_history import load_history, load_runs, record_run


def _violation(source="fred", check="freshness", severity="error", series_id="UNRATE"):
    return Violation(
        group="series",
        source=source,
        series_id=series_id,
        metric=None,
        check=check,
        severity=severity,
        detail=f"{source} is stale",
    )


def test_records_violations_and_run(tmp_path):
    ts = pd.Timestamp("2026-08-15T00:30:00Z")
    record_run([_violation(), _violation(source="cboe", severity="warn")], str(tmp_path), ts)

    hist = load_history(str(tmp_path))
    assert len(hist) == 2
    assert set(hist["source"]) == {"fred", "cboe"}
    assert hist["run_date"].unique().tolist() == ["2026-08-15"]

    runs = load_runs(str(tmp_path))
    assert len(runs) == 1
    assert runs.iloc[0]["n_violations"] == 2
    assert runs.iloc[0]["n_error"] == 1
    assert runs.iloc[0]["n_warn"] == 1


def test_clean_run_is_still_recorded(tmp_path):
    # The reason the runs table exists: with only a violations table, a clean night
    # and a night the job never ran are both "no rows", which mean opposite things.
    record_run([], str(tmp_path), pd.Timestamp("2026-08-15T00:30:00Z"))

    runs = load_runs(str(tmp_path))
    assert len(runs) == 1
    assert runs.iloc[0]["n_violations"] == 0
    assert load_history(str(tmp_path)).empty


def test_history_accumulates_across_days(tmp_path):
    record_run([_violation()], str(tmp_path), pd.Timestamp("2026-08-14T00:30:00Z"))
    record_run([_violation()], str(tmp_path), pd.Timestamp("2026-08-15T00:30:00Z"))

    hist = load_history(str(tmp_path))
    assert len(hist) == 2
    assert sorted(hist["run_date"].unique()) == ["2026-08-14", "2026-08-15"]
    # Newest first.
    assert hist.iloc[0]["run_date"] == "2026-08-15"
    assert len(load_runs(str(tmp_path))) == 2


def test_rerunning_the_same_pass_does_not_double_count(tmp_path):
    # The nightly gets re-run by hand after a fix; that must correct the day's
    # record, not append a duplicate of it.
    ts = pd.Timestamp("2026-08-15T00:30:00Z")
    record_run([_violation()], str(tmp_path), ts)
    record_run([_violation()], str(tmp_path), ts)

    assert len(load_history(str(tmp_path))) == 1
    assert len(load_runs(str(tmp_path))) == 1


def test_later_run_same_day_is_kept_separately(tmp_path):
    record_run([_violation()], str(tmp_path), pd.Timestamp("2026-08-15T00:30:00Z"))
    record_run([_violation()], str(tmp_path), pd.Timestamp("2026-08-15T09:15:00Z"))

    # Same day, different pass: both are real history.
    assert len(load_runs(str(tmp_path))) == 2
    assert len(load_history(str(tmp_path))) == 2


def test_empty_lake_returns_empty_frames(tmp_path):
    assert load_history(str(tmp_path)).empty
    assert load_runs(str(tmp_path)).empty
