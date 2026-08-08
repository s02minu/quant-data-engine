"""Tests for the events (bitemporal calendar) data-quality checks.

The checks are defense-in-depth: the ingestor produces contiguous revision_seq and
the storage upsert dedups on (event_id, revision_seq), so corruption should not
arise from the pipeline. These write malformed calendars *directly* to prove each
invariant is actually enforced.
"""

import pandas as pd

from qde.checks import run_events_checks
from qde.storage import _events_path


def _rows(rows):
    """Build a full events frame from (event_id, sched, obs, revision_seq) tuples."""
    return pd.DataFrame(
        {
            "event_id": [r[0] for r in rows],
            "series_id": [r[0].split(":")[0] for r in rows],
            "scheduled_ts": [pd.Timestamp(r[1], tz="UTC") for r in rows],
            "observed_ts": [pd.Timestamp(r[2], tz="UTC") for r in rows],
            "actual": [1.0 for _ in rows],
            "forecast": [float("nan") for _ in rows],
            "previous": [float("nan") for _ in rows],
            "revision_seq": [r[3] for r in rows],
        }
    )


def _write(base, df):
    path = _events_path("fredcal", "us_macro", base)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _checks(df, tmp_path):
    _write(str(tmp_path), df)
    return run_events_checks(str(tmp_path))


def test_clean_calendar_passes(tmp_path):
    df = _rows(
        [
            ("X:2024-01-01", "2024-02-15", "2024-02-15", 0),
            ("X:2024-01-01", "2024-02-15", "2024-03-15", 1),
            ("X:2024-02-01", "2024-03-15", "2024-03-15", 0),
        ]
    )
    assert _checks(df, tmp_path) == []


def test_observed_before_scheduled_flagged(tmp_path):
    # A value known before its release was scheduled — the lookahead-bias defect.
    df = _rows([("X:2024-01-01", "2024-02-15", "2024-02-10", 0)])
    checks = _checks(df, tmp_path)
    assert any(v.check == "bitemporal_order" and v.severity == "error" for v in checks)


def test_duplicate_initial_print_flagged(tmp_path):
    df = _rows(
        [
            ("X:2024-01-01", "2024-02-15", "2024-02-15", 0),
            ("X:2024-01-01", "2024-02-15", "2024-02-15", 0),  # two revision_seq=0
        ]
    )
    checks = _checks(df, tmp_path)
    assert any(v.check == "initial_print" for v in checks)


def test_missing_initial_print_flagged(tmp_path):
    # Revisions with no revision_seq=0 initial print.
    df = _rows(
        [
            ("X:2024-01-01", "2024-02-15", "2024-03-15", 1),
            ("X:2024-01-01", "2024-02-15", "2024-04-15", 2),
        ]
    )
    checks = _checks(df, tmp_path)
    assert any(v.check == "initial_print" for v in checks)


def test_non_contiguous_revision_seq_flagged(tmp_path):
    # An initial print exists, but revision 1 is missing (0 then 2).
    df = _rows(
        [
            ("X:2024-01-01", "2024-02-15", "2024-02-15", 0),
            ("X:2024-01-01", "2024-02-15", "2024-04-15", 2),
        ]
    )
    checks = _checks(df, tmp_path)
    assert any(v.check == "revision_seq" for v in checks)
    assert not any(v.check == "initial_print" for v in checks)  # the initial is present


def test_absent_events_yields_no_violations(tmp_path):
    assert run_events_checks(str(tmp_path)) == []  # no group=events dir at all
