"""Tests for the drafted-ingestor gauntlet.

Each case is a way a generated ingestor goes wrong *without raising* — the failure
mode that makes reviewing the code useless. A stage earns its place only by catching
one of these, so every test here is a defect the gauntlet must not let through.

Offline: the fake ingestors serve frames from memory, so nothing touches a network.
"""

import os
from pathlib import Path

import pytest

from qde.draft import GauntletReport, load_candidate, run_gauntlet
from qde.registry.spec import SourceSpec


def _spec(**over) -> SourceSpec:
    base = dict(
        group="bars",
        name="draftco",
        symbols={"XYZ": "xyz"},
        intervals=["1d"],
        max_rows_per_call=None,
        rate_limit_per_min=None,
        expected_daily_rows=1,
        null_tolerance={},
        redistributable=False,
        license_note="draft",
    )
    base.update(over)
    return SourceSpec(**base)


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "draft_ingestor.py"
    path.write_text(HEADER + body, encoding="utf-8")
    return path


HEADER = '''
import pandas as pd
from qde.ingest.base import BaseIngestor, RawPage


def _frame(dates, high=None):
    idx = pd.DatetimeIndex(pd.to_datetime(dates, utc=True), name="date")
    n = len(dates)
    return pd.DataFrame(
        {"open": [10.0] * n, "high": high or [11.0] * n, "low": [9.0] * n,
         "close": [10.5] * n, "volume": [100.0] * n},
        index=idx,
    )


DATES = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04",
         "2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08"]
'''


GOOD = '''
class DraftIngestor(BaseIngestor):
    def first_cursor(self, symbol, start, end, interval):
        return start

    def fetch_page(self, symbol, cursor, start, end, interval):
        return RawPage(rows=[(start, end)], next_cursor=None)

    def normalize(self, rows):
        start, end = rows[0]
        df = _frame(DATES)
        if start:
            df = df[df.index >= pd.Timestamp(start, tz="UTC")]
        if end:
            df = df[df.index <= pd.Timestamp(end, tz="UTC")]
        return df
'''


def test_a_correct_draft_passes_every_stage(tmp_path):
    report = run_gauntlet(
        _write(tmp_path, GOOD), _spec(), "XYZ", "2024-01-01", "2024-01-08", isolation="in-process"
    )
    assert report.passed, report.summary()
    assert {s.name for s in report.stages} >= {
        "contract", "fetch", "frame", "determinism", "range", "pagination", "cross_source"
    }


# --- each of these produces a frame a reviewer would nod at ----------------------


def test_an_ingestor_that_ignores_its_date_range_is_caught():
    """The canonical generated bug: the parameters are accepted and never used.

    It returns real data — often *more* of it — so every downstream check passes.
    Only asking for a narrow window and getting a wide one exposes it.
    """
    body = GOOD.replace(
        '''        if start:
            df = df[df.index >= pd.Timestamp(start, tz="UTC")]
        if end:
            df = df[df.index <= pd.Timestamp(end, tz="UTC")]
        return df''',
        "        return df  # start/end quietly ignored",
    )
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        report = run_gauntlet(
            _write(Path(d), body), _spec(), "XYZ",
            "2024-01-01", "2024-01-08", isolation="in-process",
        )
    failed = {s.name for s in report.failures}
    assert "range" in failed, report.summary()
    assert "ignored" in next(s for s in report.failures if s.name == "range").detail


def test_a_non_deterministic_ingestor_is_caught():
    """Two identical pulls, two different answers — each individually perfect.

    Pagination keyed to wall-clock time, an unstable sort, a cursor that skips a
    boundary row. No single frame reveals it; only asking twice does.
    """
    body = GOOD.replace(
        "    def normalize(self, rows):",
        "    _n = 0\n\n    def normalize(self, rows):",
    ).replace(
        "        df = _frame(DATES)",
        "        type(self)._n += 1\n"
        "        df = _frame(DATES if type(self)._n % 2 else DATES[:-1])",
    )
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        report = run_gauntlet(
            _write(Path(d), body), _spec(), "XYZ",
            "2024-01-01", "2024-01-08", isolation="in-process",
        )
    assert "determinism" in {s.name for s in report.failures}, report.summary()


def test_a_mismapped_price_column_is_caught():
    """`adjClose` landing on `close`, or high/low swapped: plausible numbers, wrong frame."""
    body = GOOD.replace("df = _frame(DATES)", "df = _frame(DATES, high=[1.0] * len(DATES))")
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        report = run_gauntlet(
            _write(Path(d), body), _spec(), "XYZ",
            "2024-01-01", "2024-01-08", isolation="in-process",
        )
    failed = {s.name for s in report.failures}
    assert "frame" in failed, report.summary()


def test_a_module_with_no_ingestor_fails_before_anything_runs(tmp_path):
    path = tmp_path / "draft_ingestor.py"
    path.write_text("x = 1\n", encoding="utf-8")
    report = run_gauntlet(path, _spec(), "XYZ", "2024-01-01", isolation="in-process")
    assert not report.passed
    failed = [st for st in report.stages if not st.passed]
    assert [st.name for st in failed] == ["contract"], "only the real finding, no cascade"
    assert failed[0].blocking
    # The screen runs first and passes; nothing after `contract` should have run.
    assert [st.name for st in report.stages] == ["screen", "contract"]


def test_two_ingestors_in_one_draft_is_ambiguous(tmp_path):
    path = tmp_path / "draft_ingestor.py"
    path.write_text(HEADER + GOOD + GOOD.replace("DraftIngestor", "Other"), encoding="utf-8")
    with pytest.raises(ValueError, match="exactly one"):
        load_candidate(path)


def test_the_report_is_machine_readable_so_an_agent_can_iterate(tmp_path):
    # The point of structured stages: a drafting agent reads its own failures and
    # fixes them without a person reading the code.
    report = run_gauntlet(
        _write(tmp_path, GOOD), _spec(), "XYZ", "2024-01-01", "2024-01-08", isolation="in-process"
    )
    assert isinstance(report, GauntletReport)
    assert all(isinstance(s.passed, bool) and s.detail for s in report.stages)
    assert "PASS" in report.summary()


# --- executing a draft is a decision, not a default -------------------------------


def test_an_unvetted_draft_is_contained_by_default(tmp_path, monkeypatch):
    """Isolation is the default path, not a flag someone has to remember.

    An earlier version refused to run unless a human passed --trust-this-draft. That
    put the reviewer back in the loop this module exists to remove: an agent
    iterating against the gauntlet would need approval for every attempt, or would
    pass the flag itself, which is theatre. The candidate is contained instead.
    """
    import qde.draft as draft

    marker = tmp_path / "executed.txt"
    body = GOOD + f"\n\nopen(r'{marker.as_posix()}', 'w').write('ran')\n"

    # No docker here, so the safe path is unavailable — and the honest response is to
    # fail rather than quietly fall back to executing it in this process.
    monkeypatch.setattr(draft, "_docker_available", lambda: False)
    report = run_gauntlet(_write(tmp_path, body), _spec(), "XYZ", "2024-01-01")

    assert not report.passed
    assert any(st.name == "sandbox" and st.blocking for st in report.stages)
    assert not marker.exists(), "the draft's top-level code must not have run"


def test_the_sandbox_command_is_built_without_a_secrets_mount(tmp_path, monkeypatch):
    # What actually contains a hostile draft: no bind mount except the draft itself,
    # and only its own credential in the environment.
    import qde.draft as draft

    captured = {}

    class _Done:
        returncode = 0
        stdout = '{"source":"draftco","symbol":"XYZ","group":"bars","stages":[]}'
        stderr = ""

    def _fake_run(argv, **kw):
        captured["argv"] = argv
        return _Done()

    monkeypatch.setattr(draft, "_docker_available", lambda: True)
    monkeypatch.setattr("subprocess.run", _fake_run)
    run_gauntlet(_write(tmp_path, GOOD), _spec(), "XYZ", "2024-01-01")

    argv = captured["argv"]

    # The only thing from the host filesystem is the draft itself, read-only.
    mounts = [argv[i + 1] for i, a in enumerate(argv) if a == "-v"]
    assert len(mounts) == 1 and mounts[0].endswith("candidate.py:ro"), mounts
    assert not any("secrets" in m for m in mounts), "secrets must never be mounted"

    # Exactly one credential crosses in; HOME is scratch space, not a secret.
    envs = [argv[i + 1] for i, a in enumerate(argv) if a == "-e"]
    assert "DRAFTCO_API_KEY" in envs
    assert not [e for e in envs if "KEY" in e and e != "DRAFTCO_API_KEY"], envs
    assert not [e for e in envs if "SECRET" in e or "TOKEN" in e], envs

    # Hardening: the image has no USER directive, so without --user the candidate
    # would run as root with full capabilities — the starting position for most
    # container escapes. The limits exist because this host runs the collectors.
    assert "--rm" in argv, "the container filesystem must be disposable"
    assert argv[argv.index("--user") + 1] == "65534:65534", "must not run as root"
    assert argv[argv.index("--cap-drop") + 1] == "ALL"
    assert argv[argv.index("--security-opt") + 1] == "no-new-privileges"
    assert "--read-only" in argv
    assert argv[argv.index("--pids-limit") + 1] == "256", "no fork bomb"
    assert argv[argv.index("--memory") + 1] == "2g"
    assert "-v" not in [argv[i + 1] for i, a in enumerate(argv) if a == "-v"]
    assert "/var/run/docker.sock" not in " ".join(argv), "never mount the docker socket"


def test_a_draft_reaching_for_sockets_is_refused_before_execution(tmp_path):
    marker = tmp_path / "ran.txt"
    body = (
        "import socket\n"
        + GOOD
        + f"\n\nopen(r'{marker.as_posix()}', 'w').write('ran')\n"
    )
    report = run_gauntlet(_write(tmp_path, body), _spec(), "XYZ", "2024-01-01",
                isolation="in-process",
            )

    assert not report.passed
    screen = next(st for st in report.stages if st.name == "screen")
    assert not screen.passed and "socket" in screen.detail
    assert not marker.exists(), "the screen must refuse BEFORE exec_module"


def test_the_screen_passes_the_real_ingestors():
    # A screen that flagged the project's own sources would be turned off immediately.
    from qde.draft import screen_source

    for module in ("tiingo", "binance", "fred", "cboe"):
        assert screen_source(f"src/qde/ingest/{module}.py") == [], module


def test_a_draft_sees_only_its_own_credential(tmp_path, monkeypatch):
    # A tiingo draft has to hold the Tiingo key; it has no reason to see FRED's or
    # the R2 read keys. Stripping the rest means a hostile draft can steal only the
    # secret it was already trusted with.
    from qde.draft import _only_this_sources_credentials

    monkeypatch.setenv("DRAFTCO_API_KEY", "mine")
    monkeypatch.setenv("FRED_API_KEY", "someone_elses")
    monkeypatch.setenv("QDE_R2_READ_SECRET", "infrastructure")

    with _only_this_sources_credentials("draftco"):
        assert os.environ.get("DRAFTCO_API_KEY") == "mine"
        assert os.environ.get("FRED_API_KEY") is None
        assert os.environ.get("QDE_R2_READ_SECRET") is None

    # and restored afterwards, or the rest of the process breaks
    assert os.environ.get("FRED_API_KEY") == "someone_elses"
