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

import numpy as np
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

# How far two independent sources may disagree on the same symbol before it stops
# being market structure and starts being a defect.
#
# Measured on this lake, crypto venue pairs agree to within 0.05% — a hundred
# times tighter than this threshold — which is a strong argument for tightening
# it, and a trap. The 5% is not sized for crypto: an *adjusted* equity price
# series and a raw one are both correct and drift apart by cumulative dividends,
# several percent over a few years. Tightening to what crypto shows would flag
# every legitimate adjusted-vs-raw pairing as a defect.
#
# So this stays deliberately loose and catches only gross wrongness — the wrong
# ticker, the wrong units, a different instrument. The precise instrument is the
# return correlation below, which is convention-independent and can be strict.
_CROSS_SOURCE_TOLERANCE = 0.05  # 5% median relative difference

# Two feeds on the same asset move together day by day, whatever basis separates
# their levels. Measured across every venue pair in this lake over 382 days, the
# WORST observed correlation was 0.9983 — so 0.9 sits an order of magnitude closer
# to reality than the 0.5 originally guessed, while still leaving room for a
# thinner venue or a different close convention.
#
# Unlike the level tolerance, this can be tightened safely across asset classes:
# an adjusted price series and a raw one differ in *level* by cumulative dividends,
# but their daily returns still track almost perfectly. Correlation is the one
# comparison that does not care which convention a source uses.
_MIN_RETURN_CORRELATION = 0.9

# Settled history should be byte-stable, but sources round and re-round: a close
# stored as 62824.13 can come back 62824.129999. Below this is float noise, above
# it is a genuine restatement.
_SELF_CONSISTENCY_TOLERANCE = 1e-6

# Volume is restated far more readily than price — late prints settle for hours
# after the close — so it is judged separately and loosely. A 1% shift is an
# exchange finishing its bookkeeping; a doubling is a different number entirely.
_VOLUME_REVISION_TOLERANCE = 0.01

# A scalar series is legitimately zero or negative — T10Y2Y inverts, real rates go
# below zero — so a purely relative comparison is undefined at zero and explodes near
# it. Both tolerances together: relative for the large values, absolute for the small.
# Validated against live FRED: at these settings the unrevised series (DGS10, UNRATE,
# FEDFUNDS, GDPC1) show zero changes, so nothing here is float noise, while the genuine
# revisions in PAYEMS and INDPRO are caught.
_SERIES_REVISION_RTOL = 1e-6
_SERIES_REVISION_ATOL = 1e-9

# A source that silently returns half the days looks fine to every other check —
# the rows it does return are impeccable. Judged against a peer's coverage of the
# same window, generously: venues list at different times and close on different
# calendars, so only a large shortfall means anything.
_MIN_COVERAGE_RATIO = 0.7
# Correlation over a handful of points is noise. Below this, skip it rather than
# report a number that cannot mean anything.
_MIN_DATES_FOR_CORRELATION = 6

# --- proxy bands ----------------------------------------------------------------
# Measured across all 190 pairs in this lake (up to 3,509 days each), full-history
# return correlation falls into four clearly separated bands:
#
#   same instrument, another venue   0.979 .. 0.9999   (handled by cross_check)
#   related instrument              0.585 .. 0.932     (BTC/ETH, SPY/QQQ)
#   loosely related                 0.256 .. 0.440     (crypto vs equity, GLD/TLT)
#   unrelated                      -0.172 .. 0.142     (SPY/TLT, BTC/GLD)
#
# A proxy can only ever detect a *break*; it can never confirm a price. QQQ will
# not tell you SPY closed at 512.40.
#
# 0.75 rather than the bottom of the related band, because the threshold that
# matters is not "is this pair related" but "is this pair related *stably enough*
# that a drop means something" — see _PROXY_BROKEN_MAX.
_PROXY_RELATED_MIN = 0.75

# Rolling-window floors, measured over every qualifying pair's full history. These
# are windows with NO data defect in them at all — just markets:
#
#    30d window -> legitimate correlation reached -0.268
#    60d window -> legitimate correlation reached -0.122
#    90d window -> legitimate correlation reached +0.027
#   180d window -> legitimate correlation reached +0.474
#
# So a 30-day break rule is not a check, it is a random number generator: pairs
# that correlate 0.8 for a decade spend whole months near zero. Only at 180 days
# does a floor appear that a real break could fall through.
_PROXY_RECENT_DAYS = 180
# Set below the measured 0.474 floor with room to spare. The cost of the margin is
# sensitivity — a pair drifting from 0.8 to 0.5 will not fire — and that is the
# right trade here: every fast failure mode is already covered by freshness,
# self-consistency and cross-source. Proxy is the last resort for a series nothing
# else can see, and in that role a slow check that is always right beats a fast one
# that is switched off after its third false alarm.
_PROXY_BROKEN_MAX = 0.30
# The baseline must rest on more than one market regime, or "these two are
# related" is really a statement about last quarter.
_PROXY_MIN_BASELINE_DAYS = 400

# The window is the last N *rows*, which equals the last N days only while the
# series is actually being updated. For one that stopped, the tail is a window from
# whenever it died — and a dead series' final 180 rows correlate with its peer
# perfectly well, so it passes clean while claiming to have checked "the last 180
# days". Staleness itself belongs to the freshness check; what belongs here is
# refusing to issue a verdict about a period the data does not cover. Generous,
# because a weekly pass on a daily series can legitimately see a week-old tail
# across a holiday.
_PROXY_MAX_WINDOW_AGE_DAYS = 14

# Expected spacing between rows per requested interval, for the resolution check.
_INTERVAL_SPACING = {
    "1d": pd.Timedelta(days=1),
    "1h": pd.Timedelta(hours=1),
    "1m": pd.Timedelta(minutes=1),
}


# Two different statements share the proxy tier, and conflating them is what makes a
# monitoring channel get muted. `proxy` is an *event*: a relationship that held for
# years stopped holding, and someone should look. `proxy_unavailable` is a standing
# *property of the lake*: GLD and TLT correlate with nothing else stored here, so no
# proxy verdict is possible — this week, next week, and every week until an unrelated
# decision adds a related instrument. Both are recorded; only the first is worth
# waking someone for. Recording without alerting is the point: absence of evidence
# stays queryable instead of becoming invisible or becoming noise.
PROXY = "proxy"
PROXY_UNAVAILABLE = "proxy_unavailable"


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


def cross_check(
    df: pd.DataFrame,
    symbol: str,
    source: str,
    interval: str = "1d",
    peers: list[str] | None = None,
    loader=None,
    tolerance: float = _CROSS_SOURCE_TOLERANCE,
) -> list[Violation]:
    """Compare a frame against the same symbol from an independent source.

    Everything else in this module checks a frame against *itself* — its shape,
    its internal coherence, its plausibility. All of it passes for a source that
    returns confidently wrong numbers: a stale feed, a mislabelled ticker, prices
    in pence where you expected pounds, an index where you asked for the ETF.
    Those frames are internally perfect. The only way to catch them is a second
    opinion from a source that would have to be wrong in exactly the same way.

    That is why this is the one tier that costs a network call, and why it is not
    part of :func:`verify_frame`: a nightly that double-fetched every series would
    double every rate-limit budget for a check most useful at authoring time.

    Peers come from the registry — the sources that declare the same symbol — so
    a new source is compared against whatever already covers its ground, with no
    pairing table to maintain.

    Args:
        df: the frame to check, indexed by UTC date.
        symbol / source: what this frame claims to be.
        interval: bar size, used for the peer fetch.
        peers: explicit peer sources; defaults to every other registry source
            declaring this symbol.
        loader: injected ``load_ohlcv``-alike, so tests need no network.
        tolerance: median relative difference above which the two disagree.

    Returns:
        Violations for genuine disagreement, and a ``warn`` when the comparison
        could not be made at all — an unverifiable frame and a verified one must
        never look the same.
    """
    from qde.registry import declared_series

    if loader is None:  # imported lazily so the module stays importable offline
        from qde.loaders import load_ohlcv as loader  # type: ignore[assignment]

    if peers is None:
        peers = sorted(
            {
                src
                for src, sym, iv in declared_series(group="bars")
                if sym == symbol and iv == interval and src != source
            }
        )

    if not peers:
        # Not a defect — plenty of symbols legitimately have one source. But
        # "nothing disagreed" and "nothing could disagree" are different states,
        # and collapsing them is how an unchecked source passes for a checked one.
        return [_v("bars", source, symbol, "cross_source", "warn",
                   f"no peer source declares {symbol}; this frame is unverifiable "
                   "against anything independent")]

    idx = df.index
    start, end = str(idx.min().date()), str(idx.max().date())
    ours = pd.to_numeric(df["close"], errors="coerce")

    unreachable: list[str] = []
    short_against: list[tuple[str, int]] = []
    for peer in peers:
        try:
            other = loader(symbol, start=start, end=end, interval=interval, source=peer)
        except Exception as exc:  # a peer being down says nothing about our frame
            unreachable.append(f"{peer} ({type(exc).__name__})")
            continue

        theirs = pd.to_numeric(other["close"], errors="coerce")

        # Coverage, before value. A source returning a third of the days is
        # invisible to every other check — the rows it does return are impeccable,
        # and comparing only the overlap is exactly how the gap stays hidden.
        #
        # Deferred rather than returned, for the same reason the level check
        # adjudicates: a shortfall is a statement about *two* frames. A peer that
        # answers a daily request with hourly candles has 24x our rows, and reporting
        # on its word alone convicts our correct frame of "dropping data". Held until
        # every peer has been seen, then judged on whether the peers agree.
        if len(theirs) and len(ours) / len(theirs) < _MIN_COVERAGE_RATIO:
            short_against.append((peer, len(theirs)))
            continue

        # Compare only dates both sources cover: a venue that lists later, or
        # closes on a different calendar, is not evidence of a defect.
        shared = ours.index.intersection(theirs.index)
        if len(shared) == 0:
            unreachable.append(f"{peer} (no overlapping dates)")
            continue

        a, b = ours.loc[shared], theirs.loc[shared]
        relative = ((a - b).abs() / b.abs()).dropna()
        if relative.empty:
            unreachable.append(f"{peer} (no comparable values)")
            continue

        median = float(relative.median())
        if median > tolerance:
            # Disagreement names two frames and does not say which is wrong. Ours
            # may be fine and this peer broken — reporting on one opinion turns a
            # bad peer into a false accusation against good data. A second peer
            # breaks the tie: if the peers agree with each other, the odd one out
            # is us; if they disagree, nothing here is conclusive.
            return _adjudicate(
                df, symbol, source, interval, peer, median, peers, loader, tolerance
            )

        # Levels agreeing is not enough. A stale feed serving last month's prices
        # sits well inside the tolerance whenever the asset has not moved much —
        # observed: June BTC passed a 5% level check against August. What a stale
        # feed cannot fake is *movement*: two sources tracking one asset rise and
        # fall together day by day, so their returns correlate near 1 regardless
        # of any basis between them. Correlation is what separates a live feed
        # from a frozen one that happens to be in the right neighbourhood.
        if len(shared) >= _MIN_DATES_FOR_CORRELATION:
            ours_ret, theirs_ret = a.pct_change().dropna(), b.pct_change().dropna()
            common = ours_ret.index.intersection(theirs_ret.index)
            # Zero variance means a flat series, which `_plausibility` owns;
            # correlation is undefined there rather than suspicious.
            comparable = (
                len(common) >= _MIN_DATES_FOR_CORRELATION - 1
                and ours_ret.loc[common].std() > 0
                and theirs_ret.loc[common].std() > 0
            )
            if comparable:
                corr = float(ours_ret.loc[common].corr(theirs_ret.loc[common]))
                if corr < _MIN_RETURN_CORRELATION:
                    return [_v("bars", source, symbol, "cross_source", "error",
                               f"levels match {peer} but daily moves do not "
                               f"(return correlation {corr:.2f} over {len(common)} "
                               "days) — the feed is stale, misaligned, or tracking "
                               "a different instrument")]
        # One source agreeing is enough on *value*. Polling the rest costs requests
        # and cannot strengthen a verdict that already rests on an independent feed.
        #
        # But a coverage complaint held from an earlier peer must not be thrown away
        # by this agreement: the two peers now disagree with each other about how
        # many rows the window holds, and "one peer says we are short, another does
        # not" is an unresolved conflict, not a clean bill. Returning [] here would
        # let a genuine gap vanish the moment any single peer matched on price.
        if short_against:
            names = ", ".join(f"{p} ({n} rows)" for p, n in short_against)
            return [_v("bars", source, symbol, "cross_source", "warn",
                       f"agrees with {peer} on price, but returned {len(ours)} row(s) "
                       f"against {names} for the same window — the peers disagree about "
                       "coverage, so whether this frame is missing days is unresolved")]
        return []

    # Every peer either failed on coverage or could not be reached. If more than one
    # independent peer says we are short, they are unlikely to be wrong in the same
    # way and the shortfall is ours — an error. A single peer saying so is one
    # opinion, and the peer is as likely to be the broken party, so it is reported
    # as a conflict rather than a conviction.
    if short_against:
        names = ", ".join(f"{p} ({n} rows)" for p, n in short_against)
        if len(short_against) > 1:
            return [_v("bars", source, symbol, "cross_source", "error",
                       f"returned {len(ours)} row(s) where {len(short_against)} independent "
                       f"peers have far more for the same window ({names}) — the source is "
                       "dropping data, not merely covering a different calendar")]
        return [_v("bars", source, symbol, "cross_source", "warn",
                   f"returned {len(ours)} row(s) against {names} for the same window. "
                   "One peer disagreeing on coverage does not say which of the two is "
                   "wrong; a second peer would settle it")]

    return [_v("bars", source, symbol, "cross_source", "warn",
               f"could not verify {symbol} against any peer: {', '.join(unreachable)}")]


def series_self_consistency(
    stored: pd.DataFrame,
    series_id: str,
    source: str,
    loader=None,
    rtol: float = _SERIES_REVISION_RTOL,
    atol: float = _SERIES_REVISION_ATOL,
) -> list[Violation]:
    """Detect that a stored scalar series has been revised at the source.

    The blind spot this closes is structural, not incidental. ``update_series`` is
    watermark-advanced: it fetches from the day after the newest stored
    observation, so a value it already holds is never requested again. A revision
    to an existing observation is therefore invisible to the nightly *by design* —
    and macro data revises constantly. Measured against live FRED at the time this
    was written, the lake already held a stale PAYEMS (off by 66,000 jobs) and five
    stale INDPRO values, with nothing in the platform able to see either.

    Why this is a ``warn`` and the bars equivalent is an ``error``: an exchange
    restating a settled close is a broken feed, but a statistical agency revising
    a first estimate is the data working as designed. GDP is revised for years.
    Reporting that at error severity would fire on the most important series in the
    lake, every month, and the channel would be muted inside a quarter.

    That it is normal does not make it harmless. A backtest run against the old
    numbers used values that no longer exist anywhere, and neither the lake nor the
    catalogue records which vintage it holds. So it is reported, with the
    remediation named — ``qde.backfill`` re-pulls the full history and the upsert
    is idempotent.

    Args:
        stored: the wide frame as :func:`qde.storage.load_series_local` returns it.
        series_id / source: what to re-fetch.
        loader: injected ``(series_id, source, start) -> DataFrame``, so tests need
            no network.
        rtol / atol: closeness tolerance. Both are needed: a scalar series is
            legitimately zero or negative (``T10Y2Y`` inverts), and a purely
            relative comparison is undefined at zero and explodes near it.

    Returns:
        One violation per divergence class; empty if the source reproduced what it
        said before.
    """
    if loader is None:
        def loader(series_id, source, start):  # pragma: no cover - trivial adapter
            from qde.ingest import get_ingestor

            return get_ingestor(source).load(series_id, start=start)

    settled = _settled(stored)
    if settled is None:
        return []

    start = str(settled.index.min().date())
    try:
        again = loader(series_id, source, start)
    except Exception as exc:
        return [_v("series", source, series_id, "self_consistency", "warn",
                   f"could not re-fetch {series_id} from {start} to confirm its history "
                   f"({type(exc).__name__}) — revisions unverified")]

    if again is None or again.empty or not isinstance(again.index, pd.DatetimeIndex):
        return [_v("series", source, series_id, "self_consistency", "warn",
                   f"re-fetch of {series_id} returned nothing comparable; "
                   "revisions unverified")]

    # A naive index cannot be compared against the stored UTC one at all — pandas
    # raises rather than guessing an offset. That is a real defect in the source (the
    # lake is UTC end to end), so it is reported as one instead of escaping as a
    # TypeError that says nothing about the data.
    if again.index.tz is None:
        return [_v("series", source, series_id, "self_consistency", "error",
                   f"re-fetch of {series_id} has a timezone-naive index; it cannot be "
                   "aligned with stored UTC history, so revisions are unverifiable")]

    again, out = _dedupe_refetch(again, "series", source, series_id)
    out += _row_divergence(settled, again, "series", source, series_id)

    shared = settled.index.intersection(again.index)
    if len(shared) == 0:
        return out

    # Every metric, not just the first. A multi-metric series (CFTC COT carries
    # eleven trader-category columns) can be revised in one column alone, and
    # checking one while trusting ten is a spot check reporting as a guarantee.
    revised: list[tuple[str, int, float]] = []
    compared = 0
    for column in settled.columns:
        if column not in again.columns:
            out.append(_v("series", source, series_id, "self_consistency", "error",
                          f"metric {column!r} is stored but absent from the re-fetch — "
                          "the source stopped serving a series it previously served"))
            continue

        before = pd.to_numeric(settled.loc[shared, column], errors="coerce")
        after = pd.to_numeric(again.loc[shared, column], errors="coerce")
        comparable = before.notna() & after.notna()
        if not bool(comparable.any()):
            continue

        compared += int(comparable.sum())
        x, y = before[comparable], after[comparable]
        differs = ~np.isclose(x.to_numpy(), y.to_numpy(), rtol=rtol, atol=atol)
        if differs.any():
            worst = float((x[differs] - y[differs]).abs().max())
            revised.append((str(column), int(differs.sum()), worst))

    # The mirror of a vanished metric: the source has started serving one the lake
    # does not hold. Not a defect in what is stored, but the stored series is now an
    # incomplete copy of what is available, and only a backfill will widen it.
    added = [c for c in again.columns if c not in settled.columns]
    if added:
        out.append(_v("series", source, series_id, "self_consistency", "warn",
                      f"the source now serves {len(added)} metric(s) this series does "
                      f"not store ({', '.join(map(str, added[:4]))}) — a backfill would "
                      "widen it"))

    if not compared and not out:
        # Every shared row was null on one side or the other, so the comparison ran
        # and established nothing. "Nothing disagreed" and "nothing could disagree"
        # are the two states this module exists to keep apart.
        out.append(_v("series", source, series_id, "self_consistency", "warn",
                      f"no value in {series_id} could be compared across the two "
                      "fetches (every shared date is null on one side); revisions "
                      "unverified"))

    if revised:
        revised.sort(key=lambda r: r[2], reverse=True)
        name, count, worst = revised[0]
        extra = f" (and {len(revised) - 1} other metric(s))" if len(revised) > 1 else ""
        out.append(_v("series", source, series_id, "self_consistency", "warn",
                      f"{count} settled value(s) of {name!r} have been revised since they "
                      f"were stored (largest change {worst:g}){extra} — normal for macro "
                      "data, but the stored copy is now an old vintage; re-run "
                      f"`python -m qde.backfill` for {source}/{series_id} to refresh it"))

    return out


def proxy_check(
    symbol: str,
    source: str,
    interval: str = "1d",
    candidates: list[tuple[str, str]] | None = None,
    base_dir: str = "data",
    loader=None,
) -> list[Violation]:
    """Check a peerless series against a *related* instrument instead of the same one.

    This is the tier for the case the platform will hit constantly as it grows: a
    symbol only one source carries. :func:`cross_check` has nothing to compare it
    to and honestly says so, :func:`self_consistency` proves only that the source
    is stable — a feed frozen at last Tuesday's prices is perfectly self-consistent.
    Neither notices that the series stopped tracking reality.

    A related instrument does. It cannot confirm a price — QQQ will never tell you
    what SPY closed at — but two instruments that have moved together for a year
    do not stop unless something happened, and one of the two is usually the feed.

    The relationship is **measured, never assumed**. Every candidate is scored over
    the long baseline first; only one that clears :data:`_PROXY_RELATED_MIN` earns
    the right to be evidence about anything. That ordering is the whole design: a
    check that picked its own reference would confirm whatever it was pointed at.

    What it catches, stated honestly: gross decoupling — a feed frozen at constant
    prices, one serving the wrong instrument, one returning noise. It will *not*
    catch a series drifting from 0.80 to 0.50 correlation, because real pairs do
    that on their own (see :data:`_PROXY_BROKEN_MAX`). It is a floor under the
    peerless case, not a precision instrument.

    Reads from the lake, not the network, so it costs nothing to run nightly.

    Args:
        symbol / source: the series under check.
        interval: bar size.
        candidates: explicit ``(source, symbol)`` pairs; defaults to every other
            bars series the lake holds at this interval.
        base_dir: lake root.
        loader: injected ``load_ohlcv_local``-alike, so tests need no lake.

    Returns:
        Findings under two check names, because they are two different statements.
        :data:`PROXY` is an event — a relationship that held for years stopped, or
        the feed is frozen — and is worth alerting on. :data:`PROXY_UNAVAILABLE`
        says no verdict was possible at all: no related instrument exists, the
        history is too short, or the series stopped updating so its last rows are
        not the recent period. That one is a standing property of the lake, true
        again every week, and alerting on it weekly is how a channel gets muted.
        Both are recorded; only the first should page anyone.
    """
    if loader is None:
        from qde.storage import load_ohlcv_local as loader  # type: ignore[assignment]

    if candidates is None:
        from qde.registry import declared_series

        candidates = sorted(
            {
                (src, sym)
                for src, sym, iv in declared_series(group="bars")
                if iv == interval and sym != symbol
            }
        )

    try:
        mine = _returns(loader(symbol, source=source, interval=interval, base_dir=base_dir))
    except Exception as exc:
        return [_v("bars", source, symbol, PROXY, "warn",
                   f"could not read {symbol} from the lake to proxy-check it "
                   f"({type(exc).__name__}) — relationship unverified")]

    if len(mine) < _PROXY_MIN_BASELINE_DAYS:
        return [_v("bars", source, symbol, PROXY_UNAVAILABLE, "warn",
                   f"only {len(mine)} settled day(s) of {symbol}; too short to "
                   "establish a proxy relationship, so nothing here is checked "
                   "against an external reference")]

    # A series that stopped updating still has a full tail, and that tail correlates
    # with its peer exactly as well as it always did — so it passes clean while the
    # verdict silently describes a window that ended months ago.
    age = (pd.Timestamp.now(tz="UTC").normalize() - mine.index.max()).days
    if age > _PROXY_MAX_WINDOW_AGE_DAYS:
        return [_v("bars", source, symbol, PROXY_UNAVAILABLE, "warn",
                   f"{symbol} has no data newer than {mine.index.max().date()} "
                   f"({age} days), so the most recent {_PROXY_RECENT_DAYS} rows are not "
                   "the recent period; no proxy verdict is issued for a window the "
                   "data does not cover")]

    related: list[tuple[str, float, float]] = []
    for cand_source, cand_symbol in candidates:
        try:
            theirs = _returns(
                loader(cand_symbol, source=cand_source, interval=interval, base_dir=base_dir)
            )
        except Exception:
            continue  # a candidate the lake lacks is not a finding about *this* series

        joined = pd.concat([mine, theirs], axis=1, join="inner").dropna()
        if len(joined) < _PROXY_MIN_BASELINE_DAYS + _PROXY_RECENT_DAYS:
            continue

        # The two periods must not overlap. Measured over history that *includes*
        # the window under test, a long break drags the baseline down with it, the
        # pair stops clearing the related bar, and the series is reported as having
        # no proxy rather than a broken one — the defect erases the evidence of
        # itself. Splitting them is what makes the comparison mean anything: given
        # how these two behaved before, is the recent stretch anomalous?
        past, recent = joined.iloc[:-_PROXY_RECENT_DAYS], joined.tail(_PROXY_RECENT_DAYS)
        baseline = float(past.iloc[:, 0].corr(past.iloc[:, 1]))
        if pd.isna(baseline) or baseline < _PROXY_RELATED_MIN:
            continue  # not related — it can say nothing about this series

        current = float(recent.iloc[:, 0].corr(recent.iloc[:, 1]))
        related.append((f"{cand_source}/{cand_symbol}", baseline, current))

    if not related:
        # The honest majority case for an exotic symbol, and the one worth saying
        # out loud: this series has no peer AND no usable proxy, so nothing outside
        # itself has ever confirmed it moves like the thing it claims to be.
        return [_v("bars", source, symbol, PROXY_UNAVAILABLE, "warn",
                   f"no instrument in the lake correlates with {symbol} above "
                   f"{_PROXY_RELATED_MIN:.2f} over {len(mine)} days; it is checkable "
                   "only against itself, never against the market")]

    # Judge on the strongest relationship available. A weaker one breaking too is
    # the same event seen twice, and reporting it twice buries the finding.
    name, baseline, current = max(related, key=lambda r: r[1])

    # An undefined correlation means one side had no variance at all — every return
    # identical, which for real prices means none. That is a frozen feed, the exact
    # failure this tier exists for, and it arrives looking like a missing number
    # rather than a wrong one. Treating NaN as "nothing to report" would let the
    # loudest possible defect through the quietest possible path.
    if pd.isna(current):
        return [_v("bars", source, symbol, PROXY, "error",
                   f"correlation with {name} is undefined over the last "
                   f"{_PROXY_RECENT_DAYS} days — {symbol} has no price variance at "
                   "all, which means the feed is frozen, not that the market is calm")]

    if current < _PROXY_BROKEN_MAX:
        return [_v("bars", source, symbol, PROXY, "warn",
                   f"{symbol} has tracked {name} at {baseline:.2f} correlation over its "
                   f"full history but only {current:.2f} across the last "
                   f"{_PROXY_RECENT_DAYS} days — below anything observed on a healthy "
                   "pair in this lake; the feed may have stopped following the market")]
    return []


def _returns(df: pd.DataFrame) -> pd.Series:
    """Daily close-to-close returns, indexed by date, for correlation work.

    Returns rather than levels because level is convention-dependent — an adjusted
    series and a raw one differ by cumulative dividends forever — while movement is
    not. It is the one comparison that survives two sources disagreeing about what
    a price *means*.
    """
    if df.empty or "close" not in df.columns:
        return pd.Series(dtype="float64")
    close = pd.to_numeric(df["close"], errors="coerce")
    close.index = pd.to_datetime(df.index, utc=True, errors="coerce").normalize()
    return close[close.index.notna()].sort_index().pct_change().dropna()


def _stored_bars(base_dir: str) -> set[tuple[str, str, str]]:
    """``(source, symbol, interval)`` the lake actually holds.

    A local read of partition metadata, not a fetch — cheap enough to sit in front of
    every verification claim. On failure it returns an empty set, which downgrades
    every series to its weakest honest level rather than letting an unreadable lake
    silently restore the registry's optimism.
    """
    try:
        from qde.storage import list_bars_series

        return {
            (str(r.source), str(r.symbol), str(r.interval))
            for r in list_bars_series(base_dir).itertuples(index=False)
        }
    except Exception:
        return set()


def verification_status(
    symbol: str, source: str, interval: str = "1d", base_dir: str = "data"
) -> dict:
    """How much is actually known about a series' correctness.

    Every check in this module returns evidence, never proof — and the strength of
    that evidence varies enormously by symbol. A BTC series is corroborated by six
    independent venues; SPY has exactly one source in this lake and can only ever
    be checked against itself and its neighbours. Publishing both as simply
    "stored" tells a consumer nothing about which is which.

    So verification is recorded as a *property of the data* rather than a gate it
    passed. The same instinct as ``dq_runs`` existing beside ``dq_violations``:
    absence of evidence and evidence of absence must not look identical.

    It runs no fetches, but it is **not** registry-only: a peer must both declare the
    symbol *and* actually hold data for it. The registry is a statement of intent —
    the set someone meant to backfill — and treating intent as evidence is how this
    function came to report BTCUSDT as "corroborated" by yfinance, which carries no
    BTCUSDT data at all, and ETHUSDT as corroborated by kraken and yfinance, neither
    of which has a row. Published in catalogue.json, that is precisely the flattery
    the whole tier exists to prevent, committed by the tier itself.

    Returns:
        ``{"level", "peers", "proxy_candidates", "basis"}`` — the strongest
        verification available for this series, who could provide it, and a
        sentence a human can read. ``peers`` both declare the symbol and hold data
        for it; ``proxy_candidates`` are only *eligible*
        for a proxy check, since whether any of them is actually related has to be
        measured. The distinction is the point: a field named ``proxies`` would
        claim evidence this function has not gathered.
    """
    from qde.registry import declared_series

    declared = list(declared_series(group="bars"))
    stored = _stored_bars(base_dir)
    peers = sorted(
        {
            s
            for s, sym, iv in declared
            if sym == symbol and iv == interval and s != source and (s, sym, iv) in stored
        }
    )
    # Other symbols in the lake are *candidates* for a proxy check, not proxies.
    # A related instrument can confirm a series tracks the asset class it claims —
    # measured here, SPY/QQQ correlate 0.949 against 0.998 for the same instrument
    # on another venue — but an unrelated one confirms nothing: SPY/TLT is 0.12.
    # Which candidates are actually related cannot be known from the registry, only
    # by fetching and measuring, so they are reported as candidates and never
    # counted as evidence already in hand.
    candidates = sorted({sym for _s, sym, iv in declared if iv == interval and sym != symbol})

    if peers:
        return {
            "level": "corroborated",
            "peers": peers,
            "proxy_candidates": candidates,
            "basis": (
                f"prices and daily movement confirmed against {len(peers)} independent "
                f"source(s): {', '.join(peers)}"
            ),
        }
    if candidates:
        return {
            "level": "proxy_only",
            "peers": [],
            "proxy_candidates": candidates,
            "basis": (
                "no other source carries this symbol, so its prices are not "
                "corroborated by anything. It can still be checked against itself "
                "over time, and against a related instrument if one of the "
                f"{len(candidates)} other symbols here proves correlated — which "
                "has to be measured, not assumed"
            ),
        }
    return {
        "level": "self_only",
        "peers": [],
        "proxy_candidates": [],
        "basis": (
            "nothing else in the lake can be compared against this series; only "
            "its internal contract and its agreement with itself over time"
        ),
    }


def _settled(stored: pd.DataFrame) -> pd.DataFrame | None:
    """Rows old enough to be final, or None if there is nothing to compare.

    Today's row is still forming and *should* differ between two fetches, so
    including it would make every check report a revision every time it ran.
    """
    if stored.empty or not isinstance(stored.index, pd.DatetimeIndex):
        return None  # without a date index there is nothing to line two fetches up by
    settled = stored[stored.index < pd.Timestamp.now(tz="UTC").normalize()]
    return settled if len(settled) >= 2 else None


def _dedupe_refetch(
    again: pd.DataFrame, group: str, source: str, series_id: str
) -> tuple[pd.DataFrame, list[Violation]]:
    """Collapse a re-fetch that repeats a date, and report that it did.

    A source serving the same date twice is a defect in its own right — the two
    rows may disagree, and which one is "the" value is then undefined. It also
    breaks the comparison itself: `.loc[shared]` returns more rows than `shared`
    has, so a positional comparison raises on the shape mismatch while an aligned
    one quietly compares a cartesian product. Neither is an answer.

    Deduplicated keeping the last occurrence, matching what the upsert would do, so
    the rest of the check still produces a verdict instead of only an exception.
    """
    if not again.index.has_duplicates:
        return again, []
    n = int(again.index.duplicated().sum())
    return again[~again.index.duplicated(keep="last")], [
        _v(group, source, series_id, "self_consistency", "error",
           f"the re-fetch repeats {n} date(s) (e.g. "
           f"{again.index[again.index.duplicated()][0].date()}) — the source is "
           "serving the same period more than once, so its value there is ambiguous")
    ]


def _row_divergence(
    settled: pd.DataFrame, again: pd.DataFrame, group: str, source: str, series_id: str
) -> list[Violation]:
    """Rows that disappeared or materialised, independent of any value comparison.

    Shared by both groups because the failure is identical whatever the columns
    hold: history that a source served once and no longer serves, and history it
    back-fills after the fact. Neither is visible to a check that only compares
    the rows the two fetches happen to have in common.
    """
    out: list[Violation] = []

    vanished = settled.index.difference(again.index)
    if len(vanished):
        out.append(_v(group, source, series_id, "self_consistency", "error",
                      f"{len(vanished)} date(s) present in stored history are missing "
                      f"on re-fetch (e.g. {vanished[0].date()}) — the source is "
                      "dropping history it previously served"))

    # Bounded by the stored window: rows *after* it are simply newer observations,
    # which is the normal case and not a divergence at all. Compared by membership
    # rather than a generated daily range, so a monthly or quarterly series is
    # handled the same as a daily one.
    lo, hi = settled.index.min(), settled.index.max()
    inside = again.index[(again.index >= lo) & (again.index <= hi)]
    appeared = inside.difference(settled.index)
    if len(appeared):
        out.append(_v(group, source, series_id, "self_consistency", "warn",
                      f"{len(appeared)} date(s) inside the stored window appeared only "
                      f"on re-fetch (e.g. {appeared[0].date()}) — the source back-filled "
                      "history that was absent when this series was built"))
    return out


def self_consistency(
    stored: pd.DataFrame,
    symbol: str,
    source: str,
    interval: str = "1d",
    loader=None,
    tolerance: float = _SELF_CONSISTENCY_TOLERANCE,
) -> list[Violation]:
    """Re-fetch settled history and check the source still says the same thing.

    The only verification that needs **no second source at all**, which makes it
    the one that protects every future source with no peer — the case where every
    other tier goes quiet. It rests on a property every honest feed has and no
    broken one does: *settled history does not change*. Yesterday's close was
    yesterday's close, and a source asked the same question twice must give the
    same answer.

    What it catches that nothing else can: a feed that silently revises history, a
    non-deterministic backend, pagination that drops rows depending on where the
    walk starts, a cache serving stale pages. All of those produce individually
    perfect frames — they are only visible by asking twice.

    Corporate actions are the honest exception. A split or dividend genuinely
    rewrites an adjusted series, so a real change is not automatically a defect;
    it is a fact worth surfacing, because it also silently rewrites every backtest
    that ran against the old numbers.

    Args:
        stored: the history already held, indexed by UTC date.
        symbol / source / interval: what to re-fetch.
        loader: injected ``load_ohlcv``-alike, so tests need no network.
        tolerance: relative change per row treated as noise rather than revision.

    Returns:
        A violation per divergence class found; empty if the source reproduced
        what it said before.
    """
    if loader is None:
        from qde.loaders import load_ohlcv as loader  # type: ignore[assignment]

    settled = _settled(stored)
    if settled is None:
        return []

    start, end = str(settled.index.min().date()), str(settled.index.max().date())
    try:
        again = loader(symbol, start=start, end=end, interval=interval, source=source)
    except Exception as exc:
        return [_v("bars", source, symbol, "self_consistency", "warn",
                   f"could not re-fetch {start}..{end} to confirm history "
                   f"({type(exc).__name__}) — consistency unverified")]

    again, out = _dedupe_refetch(again, "bars", source, symbol)
    out += _row_divergence(settled, again, "bars", source, symbol)

    shared = settled.index.intersection(again.index)
    if len(shared) == 0:
        return out

    # Every price column, not just close. A source is equally capable of revising
    # a high, and checking one column while trusting four is not consistency —
    # it is a spot check that reports as a guarantee.
    worst_column, worst_drift, worst_uniform = None, 0.0, False
    for column in ("open", "high", "low", "close"):
        if column not in settled.columns or column not in again.columns:
            continue
        before = pd.to_numeric(settled.loc[shared, column], errors="coerce")
        after = pd.to_numeric(again.loc[shared, column], errors="coerce")
        drift = ((before - after).abs() / after.abs()).dropna()
        changed = drift[drift > tolerance]
        if not len(changed):
            continue

        # A uniform ratio across every row is the signature of an adjustment
        # (a split or dividend restating the whole series), not corruption. Worth
        # separating: one is expected bookkeeping, the other is a broken feed —
        # and both silently invalidate a backtest run against the old values.
        ratio = (before / after).dropna()
        uniform = len(ratio) > 2 and float(ratio.std()) < 1e-6
        if float(changed.max()) > worst_drift:
            worst_column, worst_drift, worst_uniform = column, float(changed.max()), uniform

    if worst_column is not None:
        kind = (
            "a corporate action restated the series"
            if worst_uniform
            else "the source revised history"
        )
        out.append(_v("bars", source, symbol, "self_consistency",
                      "warn" if worst_uniform else "error",
                      f"settled {worst_column!r} values changed since they were stored "
                      f"(worst {worst_drift:.2%} over {len(shared)} row(s)) — {kind}; "
                      "anything backtested on the old values is now stale"))

    # Volume gets its own verdict at warn. Exchanges genuinely restate volume as
    # late prints settle, far more often than they restate a price — folding it in
    # with prices would make routine bookkeeping look like a broken feed.
    if "volume" in settled.columns and "volume" in again.columns:
        before = pd.to_numeric(settled.loc[shared, "volume"], errors="coerce")
        after = pd.to_numeric(again.loc[shared, "volume"], errors="coerce")
        vol_drift = ((before - after).abs() / after.abs()).replace(
            [float("inf"), float("-inf")], pd.NA
        ).dropna()
        vol_changed = vol_drift[vol_drift > _VOLUME_REVISION_TOLERANCE]
        if len(vol_changed):
            out.append(_v("bars", source, symbol, "self_consistency", "warn",
                          f"settled volume changed on {len(vol_changed)} row(s) "
                          f"(worst {vol_changed.max():.1%}) — usually late prints "
                          "settling, but it moves any volume-based signal"))

    return out


def _adjudicate(
    df: pd.DataFrame,
    symbol: str,
    source: str,
    interval: str,
    accuser: str,
    median: float,
    peers: list[str],
    loader,
    tolerance: float,
) -> list[Violation]:
    """Ask a third source who is wrong when our frame and a peer disagree.

    Two frames disagreeing is symmetric — it identifies a conflict, not a culprit.
    With a second peer the question becomes decidable: if the two peers agree with
    each other, our frame is the outlier; if they do not, the sources genuinely
    differ and nothing can be concluded from price alone.
    """
    ours = pd.to_numeric(df["close"], errors="coerce")
    start, end = str(df.index.min().date()), str(df.index.max().date())

    for tiebreaker in [p for p in peers if p != accuser]:
        try:
            third = pd.to_numeric(
                loader(symbol, start=start, end=end, interval=interval, source=tiebreaker)[
                    "close"
                ],
                errors="coerce",
            )
        except Exception:
            continue

        shared = ours.index.intersection(third.index)
        if len(shared) == 0:
            continue

        against_third = ((ours.loc[shared] - third.loc[shared]).abs() / third.loc[shared].abs())
        if float(against_third.dropna().median()) > tolerance:
            return [_v("bars", source, symbol, "cross_source", "error",
                       f"disagrees with {accuser} by {median:.1%} and with {tiebreaker} "
                       f"too — two independent sources agree against this frame; "
                       "check the ticker and the units")]

        return [_v("bars", source, symbol, "cross_source", "warn",
                   f"disagrees with {accuser} by {median:.1%} but matches {tiebreaker} — "
                   f"{accuser} is the likely outlier, not this frame")]

    return [_v("bars", source, symbol, "cross_source", "error",
               f"disagrees with {accuser} by {median:.1%} and no other source could "
               "adjudicate — treat as suspect until a second opinion is available")]


def _structural(df: pd.DataFrame, group: str, source: str, series_id: str) -> list[Violation]:
    """Shape: is this even the right kind of object to store?"""
    out: list[Violation] = []

    # An unrecognised group has no contract to be checked against, so every check
    # below either skips it or finds nothing — and the caller receives an empty
    # list, which in this module means "verified". A typo (`bars` -> `bar`) would
    # therefore switch verification off entirely and report success. Refusing an
    # unknown group is the difference between "nothing was wrong" and "nothing was
    # checked", which is the distinction this whole module exists to preserve.
    if group not in _REQUIRED_COLUMNS:
        return [_v(group, source, series_id, "group", "error",
                   f"unknown group {group!r}; expected one of "
                   f"{', '.join(sorted(_REQUIRED_COLUMNS))} — nothing was verified")]

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
    # `series` is checked here too. It was originally skipped, which left the group
    # with no parse check at all: a frame whose `value` column was FRED's literal
    # missing marker "." — or comma-formatted strings, or entirely null — passed
    # with zero violations. FRED's own ingestor coerces correctly, which is exactly
    # why this matters: the contract exists to hold *generated* ingestors to what
    # the hand-written one already does.
    columns = {
        "bars": ("open", "high", "low", "close", "volume"),
        "series": ("value",),
    }.get(group, ())
    if not columns:
        return out

    for column in columns:
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

        nulls = int(coerced.isna().sum())
        if not nulls:
            continue

        if group == "bars":
            # OHLCV carries no nulls: a missing price is a defect, not a gap.
            out.append(_v(group, source, series_id, "nulls", "error",
                          f"{nulls} null value(s) in {column!r}; OHLCV tolerates none"))
        elif nulls == len(coerced):
            # A scalar series legitimately has holes — FRED keeps the row and nulls
            # the value, deliberately, so the gap stays visible. Per-source null
            # tolerance is `qde.checks`' job against stored history. But a window
            # where *every* observation is missing is not a gap, it is a pull that
            # returned nothing and should have raised NoNewData.
            out.append(_v(group, source, series_id, "nulls", "warn",
                          f"all {nulls} value(s) are null; the window carries no "
                          "observation at all, which a caught-up pull should report "
                          "as NoNewData rather than as data"))

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
