"""Daily batch refresh of the lake (bars + series).

Discovers every stored bar and scalar series from the lake's partition metadata,
brings each current with a watermark-driven incremental update, then rebuilds the
quality summary CSV. One series failing (a delisted symbol, a source outage, a
missing key) is logged and skipped so it never aborts the rest of the nightly run.

Run as a module so it behaves identically on the laptop and in the container:

    python -m qde.daily_update

The base directory comes from ``QDE_BASE_DIR`` (default ``data``), matching
``qde.compact`` and ``qde.sync`` — so the same command targets ``./data`` locally
and the mounted ``/data`` volume on the VPS.
"""

import os

from qde.checks import run_checks
from qde.env import load_env_file
from qde.log import configure, get_logger
from qde.quality import build_quality_summary
from qde.registry import declared_series
from qde.storage import (
    list_bars_series,
    list_series,
    update_ohlcv,
    update_series,
)

log = get_logger(__name__)


def _log_registry_drift(base_dir: str) -> None:
    """Log any series the registry declares but the lake has not seeded yet.

    Covers both the ``bars`` and ``series`` groups. Purely informational and
    never fatal: the incremental update can only advance a series that already
    has a watermark, so seeding a newly declared one is ``qde.backfill``'s job.
    Surfacing the gap in the nightly logs is how registry drift becomes visible
    without changing what the update does.
    """
    try:
        bars = {
            (r.source, r.symbol, r.interval)
            for r in list_bars_series(base_dir).itertuples(index=False)
        }
        series = {
            (r.source, r.series_id) for r in list_series(base_dir).itertuples(index=False)
        }
        missing_bars = sorted(set(declared_series(group="bars")) - bars)
        missing_series = sorted(
            {(src, sid) for (src, sid, _iv) in declared_series(group="series")} - series
        )
        names = [f"{s}/{y}/{i}" for (s, y, i) in missing_bars]
        names += [f"{s}/{sid}" for (s, sid) in missing_series]
        if names:
            log.info("registry_unseeded", count=len(names), series=names)
    except Exception as exc:  # a drift check must never break the nightly run
        log.warning("registry_drift_check_failed", error=type(exc).__name__, detail=str(exc))


def run(base_dir: str = "data") -> dict:
    """Update every stored bar and scalar series, then run the quality checks.

    Collects the *details* of any fetch failure (not just a count) and, after the
    quality-summary rebuild, runs the registry-driven data-quality pass
    (``qde.checks``) so freshness/null violations are surfaced alongside failures.

    Returns:
        dict: ``updated`` count, ``failed`` count, the ``failures`` details, and
        the ``violations`` from the quality pass — enough for a caller to alert on.
    """
    updated = 0
    failures: list[dict] = []

    bars = list_bars_series(base_dir)
    for symbol, source, interval in zip(
        bars["symbol"], bars["source"], bars["interval"], strict=True
    ):
        try:
            update_ohlcv(symbol, source=source, interval=interval, base_dir=base_dir)
        except Exception as exc:
            failures.append(
                {
                    "group": "bars",
                    "label": f"{source}/{symbol}/{interval}",
                    "error": type(exc).__name__,
                    "detail": str(exc),
                }
            )
            log.warning(
                "update_failed",
                group="bars",
                symbol=symbol,
                source=source,
                interval=interval,
                error=type(exc).__name__,
                detail=str(exc),
            )
            continue

        updated += 1
        log.info("updated", group="bars", symbol=symbol, source=source, interval=interval)

    scalars = list_series(base_dir)
    for source, series_id in zip(scalars["source"], scalars["series_id"], strict=True):
        try:
            update_series(series_id, source=source, base_dir=base_dir)
        except Exception as exc:
            failures.append(
                {
                    "group": "series",
                    "label": f"{source}/{series_id}",
                    "error": type(exc).__name__,
                    "detail": str(exc),
                }
            )
            log.warning(
                "update_failed",
                group="series",
                series_id=series_id,
                source=source,
                error=type(exc).__name__,
                detail=str(exc),
            )
            continue

        updated += 1
        log.info("updated", group="series", series_id=series_id, source=source)

    _log_registry_drift(base_dir)
    build_quality_summary(base_dir)

    # Data-quality pass: freshness + null tolerance against each source's registry
    # contract. Read-only; a violation is logged here and surfaced by main()'s
    # alert, never fatal (a stale series must not block the compact/sync that
    # follows in maintain.sh).
    violations = run_checks(base_dir)
    for v in violations:
        log.warning(
            "dq_violation",
            group=v.group,
            series=v.label(),
            check=v.check,
            severity=v.severity,
            detail=v.detail,
        )

    return {
        "updated": updated,
        "failed": len(failures),
        "failures": failures,
        "violations": violations,
    }


def main() -> None:
    configure()
    # Load any series-source key (FRED) so the series update can run; a value
    # already exported (or set on the VPS) still wins. The optional Discord webhook
    # for health alerts loads the same way (no-op if the file is absent).
    load_env_file("secrets/fred.env")
    load_env_file("secrets/discord.env")

    base_dir = os.getenv("QDE_BASE_DIR", "data")
    summary = run(base_dir)
    log.info(
        "daily_update_complete",
        updated=summary["updated"],
        failed=summary["failed"],
        violations=len(summary["violations"]),
    )

    # Surface problems: alert only when there is a fetch failure or a DQ violation,
    # so a clean night stays silent. The exit stays 0 regardless, so maintain.sh
    # proceeds to compact + sync.
    if summary["failures"] or summary["violations"]:
        from qde.alert import format_health, send_discord

        send_discord(
            format_health(
                summary["updated"], summary["failures"], summary["violations"], base_dir=base_dir
            )
        )


if __name__ == "__main__":
    main()
