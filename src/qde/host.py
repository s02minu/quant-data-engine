"""Host health checks — the machine the pipeline runs on, not the data in it.

``qde.checks`` asks "is the data right?". This asks "is the box still able to do its
job?", which is a different failure mode and one nothing was watching: on 2026-08-15
the VPS was found at 79% disk with 24 GB of Docker build cache and only 739 MB of
actual data. Nothing would have alerted until writes started failing, and a full disk
stops ingestion, compaction, and sync alike — silently, mid-run.

Results are returned as :class:`~qde.checks.Violation` so they ride the paths that
already exist: logged by the nightly, sent to Discord by ``qde.alert``, and persisted
by ``qde.dq_history``. That last one is the real prize — it means disk pressure is
*charted over time* rather than noticed once, so a slow leak is visible as a trend
long before it is an outage.
"""

import shutil

from qde.checks import Violation
from qde.log import get_logger

log = get_logger(__name__)

# Warn early enough to act, error early enough to still have room to act *in*. A
# full disk is not a gradual degradation — writes simply start failing, so there is
# no value in thresholds that only trip once the situation is already unrecoverable.
_WARN_PCT = 80.0
_ERROR_PCT = 90.0

# Compaction rewrites a day's small files into one, so the box needs headroom beyond
# the raw growth rate. Below this, the nightly can fail even at a "fine" percentage.
_MIN_FREE_GB = 3.0


def check_disk(
    path: str = "/",
    warn_pct: float = _WARN_PCT,
    error_pct: float = _ERROR_PCT,
    min_free_gb: float = _MIN_FREE_GB,
    usage: object | None = None,
) -> list[Violation]:
    """Flag disk pressure on the filesystem holding ``path``.

    Two independent tests, because either can bite alone: a *percentage* (the tank
    is nearly full) and an *absolute floor* (a big filesystem at 85% may still have
    plenty of room; a small one at 85% may not have enough for one compaction).

    Args:
        path: any path on the filesystem to measure.
        warn_pct / error_pct: used-percentage thresholds.
        min_free_gb: absolute free-space floor, checked independently of percentage.
        usage: injected ``shutil.disk_usage``-alike, so tests need no real disk.

    Returns:
        Zero or one violation — the worst condition found, not one per rule, so a
        nearly-full disk does not produce two alerts saying the same thing.
    """
    u = usage if usage is not None else shutil.disk_usage(path)
    total = int(u.total)  # type: ignore[attr-defined]
    free = int(u.free)  # type: ignore[attr-defined]
    if total <= 0:
        # Nothing measurable. Reporting "0 GB free" here would alert on a
        # filesystem we failed to read rather than one that is actually full.
        log.warning("host_disk_unmeasurable", path=path)
        return []

    used = total - free
    pct = used / total * 100.0
    free_gb = free / 1e9

    detail = f"{pct:.0f}% used, {free_gb:.1f} GB free of {total / 1e9:.0f} GB"

    severity = None
    if pct >= error_pct or free_gb < min_free_gb / 2:
        severity = "error"
    elif pct >= warn_pct or free_gb < min_free_gb:
        severity = "warn"

    if severity is None:
        log.info("host_disk_ok", path=path, pct=round(pct, 1), free_gb=round(free_gb, 1))
        return []

    log.warning("host_disk_pressure", path=path, pct=round(pct, 1), free_gb=round(free_gb, 1))
    return [
        Violation(
            group="host",
            source="vps",
            series_id=path,
            metric=None,
            check="disk",
            severity=severity,
            detail=detail,
        )
    ]
