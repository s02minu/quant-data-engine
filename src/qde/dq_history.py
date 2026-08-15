"""Append-only history of data-quality results, so violations can be charted over time.

``qde.checks`` answers "is the lake healthy *right now*" — its :class:`~qde.checks.Violation`
objects are logged and Discord-alerted, then discarded. That is enough to wake someone up,
but it cannot answer "has FRED been flaky all month?" or "did that gap start when we added
the second venue?". This module keeps the answer.

Two tables, because one is not enough:

- **runs** — one row per nightly pass, written whether or not anything failed. Without
  this a chart cannot distinguish *zero violations* from *the job never ran*; both are
  simply an absence of violation rows, and those mean opposite things.
- **violations** — one row per failed check, carrying the run timestamp so it joins to
  the run that produced it.

Both are Hive-partitioned by UTC run date under ``<base>/quality/``, so DuckDB can read
the whole history with a single glob and the nightly only ever rewrites today's file.
"""

from pathlib import Path

import pandas as pd

from qde.checks import Violation
from qde.log import get_logger

log = get_logger(__name__)

_RUNS = "dq_runs"
_VIOLATIONS = "dq_violations"

# Written even when a run is clean, so the schema is stable for readers.
_VIOLATION_COLUMNS = [
    "run_ts",
    "run_date",
    "group",
    "source",
    "series_id",
    "metric",
    "check",
    "severity",
    "detail",
]


def _partition(base_dir: str, table: str, day: str) -> Path:
    return Path(base_dir) / "quality" / table / f"date={day}"


def _merge(path: Path, fresh: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    """Combine today's existing rows with this run's, keeping the latest per key.

    The nightly can be re-run by hand after a fix; that must correct the day's record
    rather than double-count it, so rows are replaced on ``key`` instead of appended
    blindly.
    """
    if not path.exists():
        return fresh
    try:
        prior = pd.read_parquet(path)
    except Exception as exc:  # a corrupt partition must not break the pipeline
        log.warning("dq_history_unreadable", path=str(path), error=str(exc))
        return fresh
    combined = pd.concat([prior, fresh], ignore_index=True)
    return combined.drop_duplicates(subset=key, keep="last").reset_index(drop=True)


def record_run(
    violations: list[Violation],
    base_dir: str = "data",
    run_ts: pd.Timestamp | None = None,
) -> dict:
    """Persist one data-quality pass: its summary row and any violations.

    Args:
        violations: everything ``run_checks`` and friends produced this pass.
        base_dir: local lake root.
        run_ts: the pass's timestamp; defaults to now (UTC).

    Returns:
        Summary counts, mirroring what was written.
    """
    ts = (run_ts or pd.Timestamp.now(tz="UTC")).tz_convert("UTC")
    day = ts.strftime("%Y-%m-%d")

    rows = [
        {
            "run_ts": ts,
            "run_date": day,
            "group": v.group,
            "source": v.source,
            "series_id": v.series_id,
            "metric": v.metric,
            "check": v.check,
            "severity": v.severity,
            "detail": v.detail,
        }
        for v in violations
    ]
    frame = pd.DataFrame(rows, columns=_VIOLATION_COLUMNS)

    n_error = sum(1 for v in violations if v.severity == "error")
    n_warn = sum(1 for v in violations if v.severity == "warn")
    run_row = pd.DataFrame(
        [
            {
                "run_ts": ts,
                "run_date": day,
                "n_violations": len(violations),
                "n_error": n_error,
                "n_warn": n_warn,
                "groups": ",".join(sorted({v.group for v in violations})),
            }
        ]
    )

    # The run row is always written; the violations file only when there is something
    # to say — readers glob it and an empty partition would just be noise.
    runs_dir = _partition(base_dir, _RUNS, day)
    runs_dir.mkdir(parents=True, exist_ok=True)
    runs_path = runs_dir / "runs.parquet"
    _merge(runs_path, run_row, key=["run_ts"]).to_parquet(runs_path, index=False)

    if rows:
        v_dir = _partition(base_dir, _VIOLATIONS, day)
        v_dir.mkdir(parents=True, exist_ok=True)
        v_path = v_dir / "violations.parquet"
        _merge(
            v_path,
            frame,
            key=["run_ts", "group", "source", "series_id", "metric", "check"],
        ).to_parquet(v_path, index=False)

    log.info(
        "dq_history_recorded",
        run_date=day,
        violations=len(violations),
        errors=n_error,
        warns=n_warn,
    )
    return {"run_date": day, "violations": len(violations), "errors": n_error, "warns": n_warn}


def load_history(base_dir: str = "data") -> pd.DataFrame:
    """Every recorded violation across all runs, newest first. Empty frame if none."""
    root = Path(base_dir) / "quality" / _VIOLATIONS
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame(columns=_VIOLATION_COLUMNS)
    frame = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return frame.sort_values("run_ts", ascending=False).reset_index(drop=True)


def load_runs(base_dir: str = "data") -> pd.DataFrame:
    """Every recorded run, newest first — including the clean ones. Empty frame if none."""
    root = Path(base_dir) / "quality" / _RUNS
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame(
            columns=["run_ts", "run_date", "n_violations", "n_error", "n_warn", "groups"]
        )
    frame = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return frame.sort_values("run_ts", ascending=False).reset_index(drop=True)
