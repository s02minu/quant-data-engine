"""Tests for the `events` group storage (bitemporal release calendar)."""

import pandas as pd

from qde.storage import _events_path, list_events, query, upsert_events


def _events(rows):
    """Build an events frame from (event_id, series_id, sched, obs, actual, prev, rev)."""
    cols = ["event_id", "series_id", "scheduled_ts", "observed_ts", "actual", "previous"]
    data = {c: [] for c in cols}
    data["forecast"] = []
    data["revision_seq"] = []
    for event_id, series_id, sched, obs, actual, prev, rev in rows:
        data["event_id"].append(event_id)
        data["series_id"].append(series_id)
        data["scheduled_ts"].append(pd.Timestamp(sched, tz="UTC"))
        data["observed_ts"].append(pd.Timestamp(obs, tz="UTC"))
        data["actual"].append(actual)
        data["forecast"].append(float("nan"))
        data["previous"].append(prev)
        data["revision_seq"].append(rev)
    return pd.DataFrame(data)


def test_upsert_roundtrips_and_counts(tmp_path):
    base = str(tmp_path)
    df = _events(
        [
            ("CPIAUCSL:2024-01-01", "CPIAUCSL", "2024-02-15", "2024-02-15", 3.1, None, 0),
            ("CPIAUCSL:2024-01-01", "CPIAUCSL", "2024-02-15", "2024-03-15", 3.2, None, 1),
        ]
    )
    n = upsert_events(df, "fredcal", "us_macro", base_dir=base)
    assert n == 2
    assert _events_path("fredcal", "us_macro", base).exists()


def test_new_revision_adds_a_row(tmp_path):
    base = str(tmp_path)
    upsert_events(
        _events([("X:2024-01-01", "X", "2024-02-15", "2024-02-15", 3.1, None, 0)]),
        "fredcal", "us_macro", base_dir=base,
    )
    # A later run brings a second vintage (revision 1) for the same event.
    n = upsert_events(
        _events(
            [
                ("X:2024-01-01", "X", "2024-02-15", "2024-02-15", 3.1, None, 0),
                ("X:2024-01-01", "X", "2024-02-15", "2024-03-15", 3.2, None, 1),
            ]
        ),
        "fredcal", "us_macro", base_dir=base,
    )
    assert n == 2  # the initial print plus the new revision, not duplicated


def test_upsert_is_idempotent_last_write_wins(tmp_path):
    # Re-pulling the full history is the refresh path, so it must dedup on
    # (event_id, revision_seq): a corrected value for an existing revision replaces
    # it rather than accumulating a second row.
    base = str(tmp_path)
    upsert_events(
        _events([("X:2024-01-01", "X", "2024-02-15", "2024-02-15", 3.1, None, 0)]),
        "fredcal", "us_macro", base_dir=base,
    )
    n = upsert_events(
        _events([("X:2024-01-01", "X", "2024-02-15", "2024-02-15", 9.9, None, 0)]),
        "fredcal", "us_macro", base_dir=base,
    )
    assert n == 1  # one row per (event, revision), not accumulated

    out = query(
        "SELECT actual FROM events WHERE event_id = 'X:2024-01-01' AND revision_seq = 0",
        base_dir=base,
    )
    assert out["actual"].iloc[0] == 9.9  # last write won


def test_list_events_discovers_calendars(tmp_path):
    base = str(tmp_path)
    assert list_events(base).empty  # nothing seeded yet
    upsert_events(
        _events([("X:2024-01-01", "X", "2024-02-15", "2024-02-15", 3.1, None, 0)]),
        "fredcal", "us_macro", base_dir=base,
    )
    listed = list_events(base)
    got = {(r.source, r.calendar) for r in listed.itertuples(index=False)}
    assert got == {("fredcal", "us_macro")}


def test_query_exposes_partition_keys_as_columns(tmp_path):
    base = str(tmp_path)
    upsert_events(
        _events([("X:2024-01-01", "X", "2024-02-15", "2024-02-15", 3.1, None, 0)]),
        "fredcal", "us_macro", base_dir=base,
    )
    out = query("SELECT source, calendar, event_id FROM events", base_dir=base)
    row = out.iloc[0]
    assert row["source"] == "fredcal"
    assert row["calendar"] == "us_macro"
    assert row["event_id"] == "X:2024-01-01"
