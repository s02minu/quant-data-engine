"""Daily batch refresh of the bars lake.

Discovers every stored series from the lake's partition metadata, brings each
current with a watermark-driven incremental update, then rebuilds the quality
summary CSV. One series failing (a delisted symbol, a source outage) is logged
and skipped so it never aborts the rest of the nightly run.

Run as a module so it behaves identically on the laptop and in the container:

    python -m qde.daily_update

The base directory comes from ``QDE_BASE_DIR`` (default ``data``), matching
``qde.compact`` and ``qde.sync`` — so the same command targets ``./data`` locally
and the mounted ``/data`` volume on the VPS.
"""

import os

from qde.log import configure, get_logger
from qde.quality import build_quality_summary
from qde.storage import list_bars_series, update_ohlcv

log = get_logger(__name__)


def run(base_dir: str = "data") -> dict:
    """Update every stored series, then rebuild the quality summary.

    Returns:
        dict: counts of series updated vs. skipped after a failure.
    """
    series = list_bars_series(base_dir)

    updated = 0
    failed = 0
    for symbol, source, interval in zip(
        series["symbol"], series["source"], series["interval"], strict=True
    ):
        try:
            update_ohlcv(symbol, source=source, interval=interval, base_dir=base_dir)
        except Exception as exc:
            log.warning(
                "update_failed",
                symbol=symbol,
                source=source,
                interval=interval,
                error=type(exc).__name__,
                detail=str(exc),
            )
            failed += 1
            continue

        updated += 1
        log.info("updated", symbol=symbol, source=source, interval=interval)

    build_quality_summary(base_dir)
    return {"updated": updated, "failed": failed}


def main() -> None:
    configure()
    summary = run(os.getenv("QDE_BASE_DIR", "data"))
    log.info("daily_update_complete", updated=summary["updated"], failed=summary["failed"])


if __name__ == "__main__":
    main()
