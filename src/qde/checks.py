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

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
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

    group: str  # "bars" | "series" | "events" | "microstructure"
    source: str
    series_id: str  # symbol (bars/microstructure), series_id (series), calendar (events)
    metric: str | None  # metric partition (series) or kind (microstructure), or None
    check: str  # "freshness" | "nulls" | "activity" | "gaps" | "crossed_book" | bitemporal
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


# --- events (bitemporal calendar) checks ------------------------------------


def run_events_checks(base_dir: str = "data") -> list[Violation]:
    """Validate the bitemporal integrity of every events calendar.

    Freshness/null checks are the wrong shape for a revisable release calendar;
    what must hold is the *temporal model* (docs/schemas/events.md), and getting it
    wrong bakes lookahead bias into every backtest that reads the calendar. Three
    invariants, per calendar file:

    - **bitemporal_order** — ``observed_ts >= scheduled_ts`` for every row: a value
      cannot become known before its release was scheduled to exist. This is *the*
      custom financial check the platform is built around (ROADMAP §9), an error.
    - **initial_print** — exactly one initial print (``revision_seq == 0``) per
      ``event_id``: a release has one first appearance, revisions come after.
    - **revision_seq** — the sequence is contiguous from 0 per ``event_id`` (no gap
      or duplicate), so a revision is never silently dropped or double-counted.

    A forecast/actual/previous ``NaN`` is expected (the free calendar, an
    unpublished print) and is *not* flagged here. Read-only; absent events (e.g. on
    a box that has not seeded the calendar) yields an empty list.

    Returns:
        list[Violation]: one violation per failing invariant per calendar, with an
        aggregate count in the detail rather than one alert per offending event.
    """
    root = Path(base_dir) / "bronze" / "group=events"
    if not root.exists():
        return []

    violations: list[Violation] = []
    for path in sorted(root.rglob("events.parquet")):
        keys = _partition_keys(path, root)
        source, calendar = keys.get("source", ""), keys.get("calendar", "")
        df = pd.read_parquet(path, engine="pyarrow")  # type: ignore[call-overload]
        if df.empty:
            continue

        # 1) observed_ts >= scheduled_ts (the bitemporal ordering test).
        early = int((df["observed_ts"] < df["scheduled_ts"]).sum())
        if early:
            violations.append(
                Violation(
                    "events", source, calendar, None, "bitemporal_order", "error",
                    f"{early} row(s) observed before their scheduled release "
                    f"(observed_ts < scheduled_ts)",
                )
            )

        # 2) exactly one initial print (revision_seq == 0) per event_id.
        initials = df[df["revision_seq"] == 0].groupby("event_id").size()
        all_events = df["event_id"].nunique()
        multi = int((initials > 1).sum())
        none_ = all_events - len(initials)
        if multi or none_:
            violations.append(
                Violation(
                    "events", source, calendar, None, "initial_print", "error",
                    f"{multi} event(s) with multiple initial prints, "
                    f"{none_} with none (expected exactly one revision_seq=0 each)",
                )
            )

        # 3) revision_seq contiguous from 0 per event_id (0,1,2,... no gap/dupe).
        agg = df.groupby("event_id")["revision_seq"].agg(["min", "max", "count", "nunique"])
        contiguous = (agg["min"] == 0) & (agg["max"] == agg["count"] - 1) & (
            agg["nunique"] == agg["count"]
        )
        broken = int((~contiguous).sum())
        if broken:
            violations.append(
                Violation(
                    "events", source, calendar, None, "revision_seq", "error",
                    f"{broken} event(s) with a non-contiguous revision_seq "
                    "(a gap or duplicate revision)",
                )
            )

    return violations


# --- microstructure (streamed) checks --------------------------------------

# A live pair should produce both a trade tape and a top-of-book stream; the
# absence of either on a settled day means the feed was down or partial.
_MICRO_REQUIRED_KINDS = ("trades", "book_ticker")

# Gap-record reasons, mirroring qde.stream.gaps. Kept as literals so the batch
# DQ path need not import the websocket collector stack; they are a stored-data
# contract (the values written into kind=gaps), stable regardless of the names.
_GAP_SEQUENCE_JUMP = "sequence_jump"
_GAP_RECONNECT = "reconnect"

# The session marker (start/stop liveness) is written under this synthetic symbol
# (qde.stream.collector.SESSION_SYMBOL), not a real trading pair. Kept as a literal
# -- like the gap reasons -- so the batch DQ path need not import the collector.
# The activity check exempts it: an _all partition owes no trade tape / top-of-book.
_SESSION_SYMBOL = "_all"

# Routine reconnects are expected on a 24/7 feed -- a handful a day (exchange server
# rotations, brief network blips) -- and each re-anchors from the next snapshot, so
# by themselves they drop no data and are not actionable. Alerting on them nightly
# would defeat "silent on a clean night" (an alert that fires every night is one you
# learn to ignore). They are always recorded in kind=gaps and stay queryable; a
# nightly *alert* fires only when reconnects are abnormally frequent (a flapping
# feed) -- or, separately, when any sequence jump drops data. Bias to silence.
_RECONNECT_WARN_THRESHOLD = 24


def _basis_venues() -> frozenset[str]:
    """The two venues the cross-venue basis is computed between.

    Read from ``qde.analytics`` rather than restated here, so the check and the
    mart can never disagree about which pair matters -- the same one-definition
    argument the basis mart itself makes. Guarded rather than deferred: this is
    resolved once at import, and a missing analytics module degrades the check to
    a no-op instead of taking the whole DQ pass down with it.
    """
    try:
        from qde.analytics import BASE_VENUE, QUOTE_VENUE

        return frozenset({BASE_VENUE, QUOTE_VENUE})
    except Exception:  # pragma: no cover - analytics is an in-repo module
        return frozenset()


_BASIS_VENUES = _basis_venues()


def _crossed_book_counts(root: Path, day_str: str) -> dict[tuple[str, str], tuple[int, int, int]]:
    """Count crossed books and negative sizes per (source, symbol) for one day.

    book_ticker is the chattiest kind (thousands of small part files a day), so
    this scans it with DuckDB in one streaming pass per source rather than
    reading every file into pandas — flat memory, and far faster. ``TRY_CAST``
    tolerates the string-typed prices the bronze layer keeps; ``union_by_name``
    absorbs a venue adding columns. Per-source globs (grouping by the file's own
    ``symbol`` column) avoid the hive-partition/column-name clash that a single
    ``symbol=`` hive glob would hit, since the rows already carry ``symbol``.
    Any read error yields no counts — a DQ pass must never crash the nightly.
    """
    import duckdb

    out: dict[tuple[str, str], tuple[int, int, int]] = {}
    con = duckdb.connect()
    try:
        for source_dir in sorted(root.glob("source=*")):
            source = source_dir.name.split("=", 1)[1]
            glob = (
                source_dir / f"kind=book_ticker/symbol=*/date={day_str}/*.parquet"
            ).as_posix().replace("'", "''")
            sql = f"""
                SELECT symbol,
                       count(*) FILTER (WHERE TRY_CAST(bid_price AS DOUBLE)
                                            > TRY_CAST(ask_price AS DOUBLE)) AS crossed,
                       count(*) FILTER (WHERE TRY_CAST(bid_qty AS DOUBLE) < 0
                                           OR TRY_CAST(ask_qty AS DOUBLE) < 0) AS neg,
                       count(*) AS total
                FROM read_parquet('{glob}', union_by_name=true)
                GROUP BY symbol
            """
            try:
                rows = con.execute(sql).fetchall()
            except duckdb.Error:  # no book_ticker files for this source/day
                continue
            for symbol, crossed, neg, total in rows:
                out[(source, symbol)] = (int(crossed), int(neg), int(total))
    finally:
        con.close()
    return out


def run_microstructure_checks(
    base_dir: str = "data", day: date | None = None, now: pd.Timestamp | None = None
) -> list[Violation]:
    """Data-quality checks over one settled day of streamed microstructure.

    The bars/series checks are freshness-shaped; tick data needs different ones.
    This runs over a single *settled* day (yesterday UTC by default — today's
    partition is still being written, and on the VPS yesterday's is still local
    when the nightly runs, before the compact+sync ships it):

    - **activity** — each active (source, symbol) produced both a trade tape and
      a top-of-book stream; a missing one flags a silent or partial feed.
    - **gaps** — surfaces the gap records the collector wrote live (a hole in the
      tape is invisible in row counts alone). A sequence jump is missed data
      (error); a reconnect is a known outage window (warn).
    - **crossed_book** — a book with bid > ask, or a negative size, is a data
      defect rather than a market state (error).
    - **venue_pair** — both sides of the cross-venue basis captured. A per-venue
      check cannot see this failure: each venue looks fine alone, and only the
      *pair* is the product (error).

    Read-only; returns :class:`Violation`s for the caller to log/alert on. Absent
    microstructure (e.g. on the laptop) yields an empty list.
    """
    if day is None:
        ref = now if now is not None else pd.Timestamp.now(tz="UTC")
        day = (ref - pd.Timedelta(days=1)).date()
    day_str = day.isoformat()

    root = Path(base_dir) / "bronze" / "group=microstructure"
    if not root.exists():
        return []

    # Which (source, symbol) produced which kinds on the day. Directory presence
    # is enough: flush() skips empty buffers, so a part file always has >=1 row.
    present: dict[tuple[str, str], set[str]] = defaultdict(set)
    for date_dir in root.glob(f"source=*/kind=*/symbol=*/date={day_str}"):
        keys = _partition_keys(date_dir, root)
        present[(keys.get("source", ""), keys.get("symbol", ""))].add(keys.get("kind", ""))

    if not present:
        return []

    violations: list[Violation] = []

    # 1) Feed activity. The session marker lives under the synthetic _all symbol,
    # not a trading pair, so it is exempt from the "owes trades + top-of-book" rule
    # -- otherwise every source false-flags its own session partition as a dead feed.
    for (source, symbol), kinds in sorted(present.items()):
        if symbol == _SESSION_SYMBOL:
            continue
        for required in _MICRO_REQUIRED_KINDS:
            if required not in kinds:
                violations.append(
                    Violation(
                        "microstructure", source, symbol, required, "activity", "warn",
                        f"no {required} data on {day_str} (feed silent or partial?)",
                    )
                )

    # 2) Continuity: surface the live-detected gap records.
    for (source, symbol), kinds in sorted(present.items()):
        if "gaps" not in kinds:
            continue
        frames = [
            pd.read_parquet(p, engine="pyarrow")  # type: ignore[call-overload]
            for p in root.glob(
                f"source={source}/kind=gaps/symbol={symbol}/date={day_str}/part-*.parquet"
            )
        ]
        if not frames:
            continue
        g = pd.concat(frames, ignore_index=True)
        reasons = g["reason"] if "reason" in g.columns else pd.Series(dtype=str)
        jumps = int((reasons == _GAP_SEQUENCE_JUMP).sum())
        recons = int((reasons == _GAP_RECONNECT).sum())
        missed = int(pd.to_numeric(g.get("missing_count"), errors="coerce").fillna(0).sum())
        # A sequence jump drops data (error, always surfaced). Reconnects alone are
        # only worth an alert when abnormally frequent (a flapping feed); a routine
        # handful stays silent -- still recorded in kind=gaps, just not alert-worthy.
        if jumps:
            severity = "error"
        elif recons > _RECONNECT_WARN_THRESHOLD:
            severity = "warn"
        else:
            continue
        violations.append(
            Violation(
                "microstructure", source, symbol, None, "gaps", severity,
                f"{len(g)} gap record(s) on {day_str}: {jumps} sequence-jump "
                f"(~{missed} msgs missed), {recons} reconnect",
            )
        )

    # 3) Book sanity.
    book_counts = _crossed_book_counts(root, day_str)
    for (source, symbol), (crossed, neg, total) in sorted(book_counts.items()):
        problems = []
        if crossed:
            problems.append(f"{crossed} crossed (bid>ask)")
        if neg:
            problems.append(f"{neg} negative size")
        if problems:
            violations.append(
                Violation(
                    "microstructure", source, symbol, "book_ticker", "crossed_book", "error",
                    f"{', '.join(problems)} of {total} quotes on {day_str}",
                )
            )

    # 4) Venue-pair coverage. The basis is a *relationship*, so it is the one
    # product where a healthy venue is not evidence of a healthy dataset: if one
    # collector dies, its own partitions simply stop existing, the surviving venue
    # passes every check above, and the mart quietly computes nothing for that day.
    # Checking the pair is the only way that failure has a voice.
    #
    # Judged against the venues this lake has *ever* captured, not against the pair
    # in the abstract: a single-venue deployment is a valid configuration, and a
    # check that fires nightly on one is a check you turn off. A venue only counts
    # as missing if it has a partition tree here at all -- i.e. it used to work.
    ever_captured = {d.name.split("=", 1)[1] for d in root.glob("source=*")}
    expected_pair = _BASIS_VENUES & ever_captured
    for symbol in sorted({s for (_, s) in present if s != _SESSION_SYMBOL}):
        if len(expected_pair) < 2:
            break  # this lake never ran the pair; nothing to regress from
        have = {
            source
            for (source, sym), kinds in present.items()
            if sym == symbol and "book_ticker" in kinds
        } & expected_pair
        # Neither side present is not a pair failure -- it is a symbol this pair
        # was never capturing. Only a half-captured pair is the alarming state.
        if len(have) != 1:
            continue
        violations.append(
            Violation(
                "microstructure", sorted(have)[0], symbol, None, "venue_pair", "error",
                f"only {sorted(have)[0]} captured book_ticker on {day_str}; "
                f"{sorted(expected_pair - have)[0]} is missing, so no basis for this day",
            )
        )

    return violations
