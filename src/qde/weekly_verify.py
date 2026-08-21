"""Weekly deep verification: ask every source to prove it still says the same thing.

The nightly pass (``qde.daily_update``) reads the lake. Everything it can catch is
a property of what is already stored — staleness, nulls, gaps, incoherent bars.
That is most defects, and it is cheap enough to run every night.

Three failure modes are invisible to it, and they are the ones that quietly rot a
backtest rather than breaking a job:

- **A source that revises settled history.** Yesterday's close changes; every row
  in the lake still looks perfect, because the frame the source now returns is
  perfect. Only asking twice finds it — :func:`qde.verify.self_consistency`.
- **A scalar series revised at the source.** ``update_series`` is watermark-driven:
  it fetches from the day after the newest stored observation, so a value already
  held is never asked for again. A revision to an existing observation is invisible
  to the nightly *by design* — and macro data revises constantly. When this check
  was written the lake already held a stale PAYEMS (off by 66,000 jobs) and five
  stale INDPRO values, and nothing in the platform could see either.
- **A source that stops tracking its instrument.** A frozen feed, a mislabelled
  ticker, a mirror serving stale prices. Internally coherent, plausibly ranged,
  perfectly fresh — and wrong. For a symbol with a peer, ``cross_check`` catches
  it. For a symbol with none, the only witness left is a related instrument —
  :func:`qde.verify.proxy_check`.

This runs weekly rather than nightly because the re-fetches cost a full second pull
of every series, doubling the request budget on a platform whose rate limits are the
binding constraint. Weekly is the right cadence for a defect measured in "how
stale is the backtest I ran last month", not in minutes.

Run as a module, so it behaves identically on the laptop and in the container:

    python -m qde.weekly_verify

Exits non-zero when anything error-severity is found, so cron reports it rather
than the finding sitting silently in a log.
"""

import os
import sys

from qde.dq_history import WEEKLY, record_run
from qde.env import load_secrets
from qde.log import configure, get_logger
from qde.storage import list_bars_series, list_series, load_ohlcv_local, load_series_local
from qde.verify import (
    PROXY_UNAVAILABLE,
    proxy_check,
    self_consistency,
    series_self_consistency,
)

log = get_logger(__name__)

# Only daily bars. An intraday series would re-fetch hundreds of thousands of rows
# per pass to answer the same question, and the sources that revise history revise
# it at daily granularity anyway. Scalar series carry no interval and are all checked.
_INTERVAL = "1d"


def run(
    base_dir: str = "data",
    loader=None,
    local_loader=None,
    series_loader=None,
    series_local_loader=None,
) -> dict:
    """Re-verify every stored daily bar and scalar series against its own source.

    Args:
        base_dir: local lake root.
        loader: injected network ``load_ohlcv``-alike for the re-fetch.
        local_loader: injected ``load_ohlcv_local``-alike for the proxy comparison.
        series_loader: injected ``(series_id, source, start)`` re-fetch for scalars.
        series_local_loader: injected ``load_series_local``-alike.

    Returns:
        ``checked`` (bars + scalar series), ``failed``, the ``failures`` details and
        the ``violations`` found.
    """
    read_local = local_loader or load_ohlcv_local
    read_series_local = series_local_loader or load_series_local

    violations: list = []
    checked = 0
    failures: list[dict] = []

    series = list_bars_series(base_dir)
    for symbol, source, interval in zip(
        series["symbol"], series["source"], series["interval"], strict=True
    ):
        if interval != _INTERVAL:
            continue
        label = f"{source}/{symbol}/{interval}"
        try:
            stored = read_local(symbol, source=source, interval=interval, base_dir=base_dir)
            violations += self_consistency(stored, symbol, source, interval, loader=loader)
            violations += proxy_check(
                symbol, source, interval, base_dir=base_dir, loader=local_loader
            )
        except Exception as exc:
            # A series that could not be checked is not a series that passed. It is
            # recorded as a failure so the count of *checked* stays honest, which is
            # the same reason dq_runs exists beside dq_violations.
            failures.append({"label": label, "error": type(exc).__name__, "detail": str(exc)})
            log.warning(
                "weekly_verify_failed",
                symbol=symbol,
                source=source,
                interval=interval,
                error=type(exc).__name__,
                detail=str(exc),
            )
            continue
        checked += 1

    # --- scalar series: the same question, asked of a different shape ---
    # Full history rather than a recent window: measured across every series source,
    # a complete re-fetch costs 0.4-3.0s each, so bounding the window would trade a
    # real capability — catching a benchmark revision that restates decades — for a
    # saving the box does not need.
    for source, series_id in zip(
        (scalars := list_series(base_dir))["source"], scalars["series_id"], strict=True
    ):
        label = f"{source}/{series_id}"
        try:
            stored = read_series_local(series_id, source, base_dir=base_dir)
            violations += series_self_consistency(
                stored, series_id, source, loader=series_loader
            )
        except Exception as exc:
            failures.append({"label": label, "error": type(exc).__name__, "detail": str(exc)})
            log.warning(
                "weekly_verify_failed",
                group="series",
                series_id=series_id,
                source=source,
                error=type(exc).__name__,
                detail=str(exc),
            )
            continue
        checked += 1

    for v in violations:
        log.warning(
            "dq_violation",
            group=v.group,
            series=v.label(),
            check=v.check,
            severity=v.severity,
            detail=v.detail,
        )

    return {"checked": checked, "failed": len(failures), "failures": failures,
            "violations": violations}


def main() -> None:
    configure()
    load_secrets()

    base_dir = os.getenv("QDE_BASE_DIR", "data")
    summary = run(base_dir)
    errors = [v for v in summary["violations"] if v.severity == "error"]

    log.info(
        "weekly_verify_complete",
        checked=summary["checked"],
        failed=summary["failed"],
        violations=len(summary["violations"]),
        errors=len(errors),
    )

    # Recorded whether or not anything failed: a week with no violations and a week
    # the job never ran are both an absence of violation rows, and they mean
    # opposite things. Non-fatal — a bookkeeping failure must not swallow the
    # findings it was meant to store.
    try:
        record_run(summary["violations"], base_dir, cadence=WEEKLY)
    except Exception as exc:
        log.warning("dq_history_failed", error=str(exc))

    # Everything is recorded above; only *actionable* findings are sent. The lake
    # permanently contains series with no usable proxy — GLD and TLT correlate with
    # nothing else stored here — and those produce the same violation every single
    # week, forever, with nothing anyone can do about it. Alerting on them would
    # train the reader to dismiss the channel, and the one week it carried a frozen
    # feed would be dismissed with the rest. They stay queryable in dq_violations,
    # which is where a standing property belongs.
    actionable = [v for v in summary["violations"] if v.check != PROXY_UNAVAILABLE]
    if summary["failures"] or actionable:
        from qde.alert import format_health, send_discord

        send_discord(
            format_health(
                summary["checked"],
                summary["failures"],
                actionable,
                base_dir=base_dir,
                title="qde weekly verification",
                verb="checked",
            )
        )

    # Non-zero only on error severity. A warn here is usually the honest
    # "unverifiable" state — a symbol with no peer and no usable proxy — which is
    # a fact about the lake's coverage, not a fault to page on every week.
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
