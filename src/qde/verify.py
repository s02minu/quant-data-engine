"""Verify a frame against its group's contract, before it is ever stored.

``qde.checks`` asks "is the lake healthy?" — it walks partitions on disk and
judges what is already there. This asks the earlier question: **is this frame
fit to become lake data at all?** Same invariants, applied to a DataFrame in
memory, with no path, no partition, and no prior state.

That split matters for two callers:

- **Generated ingestors.** Code drafted from an API's documentation gets the
  response shape wrong in ways that do not raise: a field renamed since the docs
  were written, an adjusted close mapped onto ``close``, a millisecond timestamp
  read as seconds. Every one of those produces a frame that looks entirely
  plausible. Reviewing the code will not catch a field mismapping — a human
  reading ``df["close"]`` nods. ``high >= close`` does not.
- **The nightly.** ``run_checks`` reads the lake to apply the same rules, which
  means testing them requires building a lake on disk. These functions take a
  frame, so the contract can be tested directly.

Deliberately *not* here: freshness and cadence. Those are properties of a
series' history, not of a frame, and they belong to ``qde.checks`` where the
stored data is in hand.
"""

import pandas as pd

from qde.checks import Violation

# OHLC coherence uses a relative epsilon for the same reason the dbt test does
# (`transform/tests/assert_ohlc_coherent.sql`): yfinance's dividend adjustment
# leaves floating-point noise around 1e-16 relative, which trips a strict
# comparison. A real defect — a bad print, a decimal shift, a mismapped column —
# is off by 1e-4 relative or worse, orders of magnitude above this floor.
_OHLC_TOLERANCE = 1e-6

# Canonical columns per group, as the storage layer expects them. A frame
# missing one of these cannot be stored correctly no matter how clean it looks.
_REQUIRED_COLUMNS = {
    "bars": ("open", "high", "low", "close", "volume"),
    "series": ("value",),
    "events": ("event_id", "revision_seq", "scheduled_ts", "observed_ts"),
}

# A daily move beyond this is almost certainly a units error, a decimal shift, or
# a mismapped column rather than a real market move. Crypto genuinely moves hard,
# so the bar is set where no legitimate daily close-to-close return reaches it.
_IMPLAUSIBLE_DAILY_RETURN = 5.0  # +400% / -80% in one day

# Expected spacing between rows per requested interval, for the resolution check.
_INTERVAL_SPACING = {
    "1d": pd.Timedelta(days=1),
    "1h": pd.Timedelta(hours=1),
    "1m": pd.Timedelta(minutes=1),
}


def _v(
    group: str, source: str, series_id: str, check: str, severity: str, detail: str
) -> Violation:
    return Violation(group, source, series_id, None, check, severity, detail)


def verify_frame(
    df: pd.DataFrame,
    group: str,
    source: str = "?",
    series_id: str = "?",
    start: str | None = None,
    end: str | None = None,
    interval: str | None = None,
) -> list[Violation]:
    """Check a frame against everything its group's contract can decide alone.

    Ordered cheapest-first, and deliberately *not* short-circuiting: a caller
    drafting an ingestor wants every problem in one pass, not the first one.

    Args:
        df: the frame as the ingestor's ``normalize`` produced it.
        group: ``bars`` | ``series`` | ``events``.
        source / series_id: identity, for the returned violations only.
        start / end: the range that was *requested*. Supplying them turns on the
            range check, which is the only thing that catches an ingestor
            ignoring its date parameters — and the only reliable way to catch an
            epoch-unit error that lands in 1970 rather than the far future.
        interval: the bar size that was requested (``"1d"``). Supplying it turns
            on the spacing check.

    Returns:
        Every violation found. Empty means the frame satisfies its contract —
        which is a statement about shape and internal consistency, not proof the
        numbers are right. Cross-source agreement is the check for that, and it
        needs a second source rather than a second look at this one.
    """
    violations: list[Violation] = []
    violations += _structural(df, group, source, series_id)
    # A frame that failed structurally will fail the rest in cascade, and those
    # follow-on violations are noise that buries the real finding.
    if violations:
        return violations

    violations += _timestamps(df, group, source, series_id, start, end, interval)
    violations += _parseable(df, group, source, series_id)
    violations += _contract(df, group, source, series_id)
    violations += _plausibility(df, group, source, series_id)
    return violations


def _structural(df: pd.DataFrame, group: str, source: str, series_id: str) -> list[Violation]:
    """Shape: is this even the right kind of object to store?"""
    out: list[Violation] = []

    if df.empty:
        # Not a defect on its own — the ingestor contract raises NoNewData for a
        # caught-up pull — but a frame that reached verification empty means the
        # caller skipped that path.
        return [_v(group, source, series_id, "empty", "error",
                   "frame has no rows; a caught-up pull should raise NoNewData instead")]

    missing = [c for c in _REQUIRED_COLUMNS.get(group, ()) if c not in df.columns]
    if missing:
        out.append(_v(group, source, series_id, "columns", "error",
                      f"missing required column(s) for group={group}: {', '.join(missing)}"))

    # bars and series are keyed by a UTC date index; events are keyed by columns.
    if group in ("bars", "series"):
        idx = df.index
        if not isinstance(idx, pd.DatetimeIndex):
            out.append(_v(group, source, series_id, "index", "error",
                          f"index is {type(idx).__name__}, expected a DatetimeIndex"))
            return out  # every check below reads the index

        if idx.tz is None:
            # A naive index silently means "local time" to pandas, and the lake is
            # UTC throughout. Storing one shifts every timestamp by the reader's
            # offset without a single error.
            out.append(_v(group, source, series_id, "index", "error",
                          "index is timezone-naive; the lake is UTC end to end"))
        if idx.has_duplicates:
            n = int(idx.duplicated().sum())
            out.append(_v(group, source, series_id, "index", "error",
                          f"{n} duplicate index value(s); the upsert would silently drop them"))
        if not idx.is_monotonic_increasing:
            out.append(_v(group, source, series_id, "index", "warn",
                          "index is not sorted ascending"))

    return out


def _timestamps(
    df: pd.DataFrame,
    group: str,
    source: str,
    series_id: str,
    start: str | None,
    end: str | None,
    interval: str | None,
) -> list[Violation]:
    """Catch the epoch-unit errors, which produce a perfectly valid index.

    Reading millisecond timestamps as seconds throws every date into the year
    55000; reading seconds as milliseconds collapses them all into January 1970.
    Both build a clean, sorted, timezone-aware ``DatetimeIndex`` that satisfies
    every structural check — the frame is only wrong once you know what range was
    asked for. That is why ``start``/``end`` matter more here than they look.
    """
    out: list[Violation] = []
    if group == "events" or not isinstance(df.index, pd.DatetimeIndex):
        return out

    idx = df.index

    # No bar can exist for a day that has not elapsed. Same reasoning as the guard
    # in `storage.update_ohlcv`, applied to the data instead of the request. The
    # allowance covers a venue stamping a still-forming daily bar.
    horizon = pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=2)
    if bool((idx > horizon).any()):
        out.append(_v(group, source, series_id, "future_dates", "error",
                      f"{int((idx > horizon).sum())} row(s) dated in the future "
                      f"(latest {idx.max()}) — usually epoch seconds read as milliseconds"))

    # Data outside the window that was asked for means the source ignored the date
    # parameters, or the parameters were built in the wrong unit.
    if start is not None:
        lower = pd.Timestamp(start, tz="UTC")
        before = idx < lower
        if bool(before.any()):
            out.append(_v(group, source, series_id, "range", "error",
                          f"{int(before.sum())} row(s) before the requested start {start} "
                          f"(earliest {idx.min()}) — the range parameters were ignored "
                          "or built in the wrong unit"))
    if end is not None:
        upper = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)
        after = idx > upper
        if bool(after.any()):
            out.append(_v(group, source, series_id, "range", "error",
                          f"{int(after.sum())} row(s) after the requested end {end} "
                          f"(latest {idx.max()})"))

    # A frame of hourly candles satisfies every other check when daily was asked
    # for — the numbers are real, they are simply the wrong series.
    if interval is not None and len(idx) >= 3:
        expected = _INTERVAL_SPACING.get(interval)
        if expected is not None:
            gaps = pd.Series(idx).diff().dropna()
            if not gaps.empty:
                modal = gaps.mode()
                if not modal.empty and modal.iloc[0] != expected:
                    out.append(_v(group, source, series_id, "interval", "error",
                                  f"rows are {modal.iloc[0]} apart but interval={interval} "
                                  f"was requested — wrong resolution"))

    return out


def _parseable(df: pd.DataFrame, group: str, source: str, series_id: str) -> list[Violation]:
    """Catch numeric columns that are present but do not survive coercion.

    Every numeric check downstream coerces with ``errors="coerce"``, which turns
    an unparseable value into NaN — and NaN compares false against everything, so
    a price column of ``"1,234.56"`` strings sails through OHLC coherence,
    variance, and returns without a single violation. The column has to be checked
    for *parseability* before anything is allowed to compare it.
    """
    out: list[Violation] = []
    if group != "bars":
        return out

    for column in ("open", "high", "low", "close", "volume"):
        if column not in df.columns:
            continue
        raw = df[column]
        coerced = pd.to_numeric(raw, errors="coerce")

        # Present in the source but destroyed by coercion: a parse failure, not a
        # missing value. Worth separating, because the fixes differ.
        unparseable = int((raw.notna() & coerced.isna()).sum())
        if unparseable:
            sample = raw[raw.notna() & coerced.isna()].iloc[0]
            out.append(_v(group, source, series_id, "numeric", "error",
                          f"{unparseable} value(s) in {column!r} are not numeric "
                          f"(e.g. {sample!r}) — needs parsing before storage"))
            continue

        # OHLCV carries no nulls: a missing price is a defect, not a gap.
        nulls = int(coerced.isna().sum())
        if nulls:
            out.append(_v(group, source, series_id, "nulls", "error",
                          f"{nulls} null value(s) in {column!r}; OHLCV tolerates none"))

    return out


def _contract(df: pd.DataFrame, group: str, source: str, series_id: str) -> list[Violation]:
    """Invariants that define the group — the same ones dbt asserts over gold."""
    out: list[Violation] = []

    if group == "bars":
        num = df[["open", "high", "low", "close"]].apply(pd.to_numeric, errors="coerce")
        tol = _OHLC_TOLERANCE * num["close"].abs()

        # The high is the max and the low is the min, by definition. This is the
        # check that catches a mismapped price column — the single most likely
        # error in a generated ingestor, and one that produces entirely plausible
        # numbers when it happens.
        incoherent = (
            (num["high"] < num["low"] - tol)
            | (num["high"] < num["open"] - tol)
            | (num["high"] < num["close"] - tol)
            | (num["low"] > num["open"] + tol)
            | (num["low"] > num["close"] + tol)
        )
        if bool(incoherent.any()):
            n = int(incoherent.sum())
            first = df.index[incoherent][0]
            out.append(_v(group, source, series_id, "ohlc_coherent", "error",
                          f"{n} row(s) where high is not the max or low not the min "
                          f"(first: {first}) — usually a mismapped price column"))

        volume = pd.to_numeric(df["volume"], errors="coerce")
        if bool((volume < 0).any()):
            out.append(_v(group, source, series_id, "volume", "error",
                          f"{int((volume < 0).sum())} row(s) with negative volume"))

    if group == "events":
        # The bitemporal ordering rule (ROADMAP §9): a release cannot be observed
        # before it was scheduled. Restated here so a generated events ingestor is
        # held to it at authoring time, not only by the nightly.
        scheduled = pd.to_datetime(df["scheduled_ts"], utc=True, errors="coerce")
        observed = pd.to_datetime(df["observed_ts"], utc=True, errors="coerce")
        backwards = observed < scheduled
        if bool(backwards.any()):
            out.append(_v(group, source, series_id, "bitemporal", "error",
                          f"{int(backwards.sum())} row(s) observed before scheduled"))

        # Events are keyed by (event_id, revision_seq) rather than an index, so the
        # duplicate check above cannot see them. A repeated key means the upsert
        # silently keeps one row and discards the other.
        duplicated = int(df.duplicated(subset=["event_id", "revision_seq"]).sum())
        if duplicated:
            out.append(_v(group, source, series_id, "key", "error",
                          f"{duplicated} duplicate (event_id, revision_seq) pair(s); "
                          "the upsert would drop one silently"))

        # Revisions number from 0 upward with no holes. A gap means a vintage was
        # missed — the whole point of the bitemporal group is that the sequence is
        # complete, so an incomplete one is worse than none.
        for event_id, chunk in df.groupby("event_id"):
            seqs = sorted(pd.to_numeric(chunk["revision_seq"], errors="coerce").dropna())
            if seqs != list(range(len(seqs))):
                out.append(_v(group, source, str(event_id), "revision_seq", "error",
                              f"revision_seq is {seqs}, expected a contiguous run from 0"))
                break  # one example is enough to act on

    return out


def _plausibility(df: pd.DataFrame, group: str, source: str, series_id: str) -> list[Violation]:
    """Statistical smell tests — warnings, because markets do surprise you."""
    out: list[Violation] = []
    if group != "bars" or len(df) < 3:
        return out

    close = pd.to_numeric(df["close"], errors="coerce")

    returns = close.pct_change().abs()
    if bool((returns > _IMPLAUSIBLE_DAILY_RETURN).any()):
        worst = float(returns.max())
        out.append(_v(group, source, series_id, "returns", "warn",
                      f"largest single-period move is {worst:.0%} — check for a units "
                      "error, a decimal shift, or an interval mismatch"))

    # A price column that never moves is the signature of reading a constant
    # field — a limit, a divisor, a placeholder — instead of the price.
    if close.notna().any() and float(close.std(skipna=True) or 0.0) == 0.0:
        out.append(_v(group, source, series_id, "variance", "warn",
                      f"close is constant at {close.dropna().iloc[0]} across "
                      f"{len(df)} rows — likely the wrong field"))

    return out
