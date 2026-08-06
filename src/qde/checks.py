"""Registry-driven data-quality checks over the lake (Phase 9).

The platform now runs unattended, so the question is not "does the pipeline run"
but "is the data it lands still *correct and fresh*". These checks answer that by
walking the seeded lake and testing each series against the contract its
:class:`~qde.registry.spec.SourceSpec` already declares — the same thresholds that
configure the ingestors, now enforced instead of merely documented.

Two checks, the ones that matter once data is flowing:

- **Freshness** — has the series gone stale? Rather than a fixed "N days old"
  rule (wrong the moment sources have different cadences — daily CBOE, weekly
  CFTC, 8-hourly funding, monthly/quarterly FRED), staleness is judged against
  the series' *own* typical spacing, derived from its recent observations. So the
  check self-calibrates per series and needs no per-series frequency metadata,
  with the registry's ``freshness_sla_minutes`` as a floor.
- **Null rate** — does a column breach the source's declared ``null_tolerance``?
  A missing value is legitimate for some sources (FRED's "not yet published") and
  a defect for others (a price), which is exactly what the per-column tolerance
  encodes.

The checks are read-only and return a list of :class:`Violation`; surfacing them
(logs, a Discord alert) is the caller's job (see ``qde.daily_update`` / ``qde.alert``).
"""

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from qde.registry import SOURCES

# How many typical gaps a series may fall behind before it is called stale. The
# gap estimate is a high percentile of recent spacings, so ordinary weekend and
# holiday gaps are already absorbed; this factor is headroom on top of that. Set
# generously (3x) on purpose: many series are dated by *period start* but published
# with a lag (a June CPI print lands in mid-July and is dated 06-01), so the age of
# the newest observation runs a full period + lag behind even when perfectly
# current. An unattended nightly alert must bias to silence — a source that truly
# breaks keeps aging and still trips the threshold within a few periods.
_STALE_FACTOR = 3.0
# How many recent observations to estimate current cadence from — enough to be
# stable, few enough to reflect a series that changed frequency.
_CADENCE_WINDOW = 30


@dataclass
class Violation:
    """One failed data-quality check, ready to log or render into an alert."""

    group: str  # "bars" | "series"
    source: str
    series_id: str  # symbol (bars) or series_id (series)
    metric: str | None  # the metric partition, or None
    check: str  # "freshness" | "nulls"
    severity: str  # "error" | "warn"
    detail: str

    def label(self) -> str:
        """A compact identifier for logs/alerts, e.g. ``cftc/ES/dealer_long``."""
        parts = [self.source, self.series_id]
        if self.metric:
            parts.append(self.metric)
        return "/".join(parts)


def _freshness_detail(
    dates: pd.DatetimeIndex, now: pd.Timestamp, sla_floor: pd.Timedelta
) -> str | None:
    """Return a staleness message if the newest observation is overdue, else None.

    The threshold is ``max(floor, factor x typical_gap)``, where ``typical_gap`` is
    a high percentile of the recent inter-observation spacings — so a daily series
    tolerates weekends, a weekly one tolerates ~a fortnight, and an 8-hourly one is
    held to hours, all without being told its frequency.
    """
    if len(dates) < 3:
        return None  # too few points to infer a cadence; nothing to judge against

    ordered = dates.sort_values()
    gaps = ordered.to_series().diff().dropna().tail(_CADENCE_WINDOW)
    typical = pd.Timedelta(gaps.quantile(0.9))  # the largest *ordinary* gap (absorbs weekends)
    threshold: pd.Timedelta = max(typical * _STALE_FACTOR, sla_floor)

    age = now - ordered[-1]
    if age > threshold:
        return (
            f"last observation {ordered[-1].date()} is {age.components.days}d "
            f"{age.components.hours}h old (expected within ~{threshold})"
        )
    return None


def _null_details(df: pd.DataFrame, tolerance: dict[str, float]) -> list[tuple[str, str]]:
    """Return ``(column, message)`` for every column breaching its null tolerance.

    Only columns with a declared tolerance are checked — the registry decides
    which nulls are defects (a price) versus legitimate gaps (an unpublished macro
    print). An empty frame yields nothing (freshness owns the "no data" case).
    """
    if df.empty:
        return []
    out = []
    for column, limit in tolerance.items():
        if column not in df.columns:
            continue
        fraction = float(df[column].isna().mean())
        if fraction > limit:
            out.append((column, f"{column} nulls {fraction:.1%} exceed tolerance {limit:.0%}"))
    return out


def _partition_keys(path: Path, root: Path) -> dict[str, str]:
    """Parse ``key=value`` partition segments from a file's path under ``root``."""
    return dict(
        segment.split("=", 1)
        for segment in path.relative_to(root).parts
        if "=" in segment
    )


def _sla_floor(source: str) -> pd.Timedelta:
    spec = SOURCES.get(source)
    minutes = spec.freshness_sla_minutes if spec else 24 * 60 + 60
    return pd.Timedelta(minutes=minutes)


def _tolerance(source: str) -> dict[str, float]:
    spec = SOURCES.get(source)
    return spec.null_tolerance if spec else {}


def run_checks(base_dir: str = "data", now: pd.Timestamp | None = None) -> list[Violation]:
    """Run freshness + null checks over every seeded bar and scalar series.

    Reads each series file once, judged against its source's registry contract.
    For a multi-metric series (CFTC, perps) freshness is checked once per
    ``(source, series_id)`` — the metrics share report dates, so one stale market
    is one violation, not eleven — while nulls are checked per metric.

    Returns:
        list[Violation]: every check that failed, most-severe intent first is the
        caller's to sort; order here is series-then-bars, discovery order.
    """
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    violations: list[Violation] = []

    # --- series group (flat FRED/CBOE and metric CFTC/perp files alike) ---
    series_root = Path(base_dir) / "bronze" / "group=series"
    fresh_checked: set[tuple[str, str]] = set()
    for path in sorted(series_root.rglob("series.parquet")):
        keys = _partition_keys(path, series_root)
        source, series_id = keys.get("source", ""), keys.get("series_id", "")
        metric = keys.get("metric")
        df = pd.read_parquet(path, engine="pyarrow")  # type: ignore[call-overload]

        if (source, series_id) not in fresh_checked:
            fresh_checked.add((source, series_id))
            detail = _freshness_detail(pd.DatetimeIndex(df.index), now, _sla_floor(source))
            if detail:
                violations.append(
                    Violation("series", source, series_id, None, "freshness", "warn", detail)
                )

        for _column, message in _null_details(df, _tolerance(source)):
            violations.append(
                Violation("series", source, series_id, metric, "nulls", "error", message)
            )

    # --- bars group (one file per source/symbol/interval, no metric) ---
    bars_root = Path(base_dir) / "bronze" / "group=bars"
    for path in sorted(bars_root.rglob("bars.parquet")):
        keys = _partition_keys(path, bars_root)
        source, symbol = keys.get("source", ""), keys.get("symbol", "")
        df = pd.read_parquet(path, engine="pyarrow")  # type: ignore[call-overload]

        detail = _freshness_detail(pd.DatetimeIndex(df.index), now, _sla_floor(source))
        if detail:
            violations.append(Violation("bars", source, symbol, None, "freshness", "warn", detail))

        for _column, message in _null_details(df, _tolerance(source)):
            violations.append(Violation("bars", source, symbol, None, "nulls", "error", message))

    return violations
