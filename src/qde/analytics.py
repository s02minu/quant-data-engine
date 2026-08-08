"""Cross-venue microstructure analytics -- the first *consumer* of the lake.

Everything up to here lands and guards raw data; this is the first module that
*reads* it to produce a signal, and it targets the platform's wedge: not
single-venue tick data (a commodity), but the relationship *between* venues.

Two views over the top-of-book of the same instrument on two venues -- Binance
BTC/USDT (offshore, stablecoin-denominated) and Coinbase BTC/USD (US, fiat):

- **Basis** -- Coinbase mid minus Binance mid, in basis points. This is the
  USD/USDT stablecoin premium plus the US-vs-offshore flow difference: when USDT
  trades below \\$1 (risk-off), a USDT-denominated BTC price prints high and the
  basis turns negative; a US bid (ETF flow, a fiat on-ramp surge) pushes it
  positive. A number single-venue data cannot show.
- **Lead-lag** -- whose returns move first. Cross-correlate the two venues'
  bucketed mid returns across a range of lags; the lag of peak correlation says
  which venue leads and by how long. The more liquid venue usually leads, and
  *how much* it leads is itself a regime signal.

Both align on our collector's ``received_at`` (a millisecond epoch stamped by the
*same* VPS clock for every venue), so the comparison is free of exchange
clock-skew -- the one time reference that is apples-to-apples across venues.

The heavy step -- collapsing millions of ``book_ticker`` rows to one mid per time
bucket -- runs in DuckDB (``arg_max(mid, received_at)`` = the last mid in the
bucket), so only the small bucketed series crosses the wire; the pure-pandas math
downstream is unit-tested offline. Read-only: no writes, a query the lake serves.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import cast

import numpy as np
import pandas as pd

from qde import lake

# The venues whose books we compare, base first. Base is the denominator of the
# basis and the reference leg of the lead-lag; Binance leads by liquidity, so it
# is the natural base (Coinbase measured relative to it).
BASE_VENUE = "binance"
QUOTE_VENUE = "coinbase"


def _mids_sql(symbol: str, day: str, bucket_ms: int, sources: tuple[str, ...]) -> str:
    """SQL collapsing book_ticker to one mid per (source, time bucket).

    ``arg_max(mid, received_at)`` keeps the *last* mid seen in each bucket (a
    quote holds until the next), and ``TRY_CAST`` tolerates the string-typed
    bronze prices. Grouping in DuckDB means only the bucketed series is
    transferred, not the millions of underlying quotes.
    """
    src_list = ", ".join(f"'{s}'" for s in sources)
    return f"""
        SELECT source,
               received_at // {int(bucket_ms)} AS bucket,
               arg_max(
                   (TRY_CAST(bid_price AS DOUBLE) + TRY_CAST(ask_price AS DOUBLE)) / 2,
                   received_at
               ) AS mid
        FROM book_ticker
        WHERE symbol = '{symbol}' AND date = DATE '{day}' AND source IN ({src_list})
        GROUP BY source, bucket
        ORDER BY bucket
    """


def load_mids(
    symbol: str = "BTCUSDT",
    day: str = "",
    bucket_ms: int = 1000,
    sources: tuple[str, ...] = (BASE_VENUE, QUOTE_VENUE),
    query: Callable[[str], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Load one mid per venue per time bucket for a symbol on a day.

    Returns a *wide* frame indexed by ``bucket`` (integer ``received_at //
    bucket_ms``) with one column per source. Buckets a venue never updated in are
    absent (NaN); :func:`align` regularizes them.
    """
    day = day or _yesterday_utc()
    q = query or lake.query
    long = q(_mids_sql(symbol, day, bucket_ms, sources))
    if long.empty:
        return pd.DataFrame(columns=list(sources))
    return long.pivot(index="bucket", columns="source", values="mid").sort_index()


def align(wide: pd.DataFrame) -> pd.DataFrame:
    """Regularize venue mids onto a common, gap-free bucket grid over their overlap.

    Two fixes make the venues comparable: (1) reindex to *every* bucket in range
    and forward-fill, so a bucket with no update on one venue still carries its
    prevailing mid and a one-row shift is exactly one bucket of time (needed for
    lead-lag); (2) restrict to the window where every venue has started and none
    has ended -- the true overlap -- so the edges are not one venue alone.
    """
    if wide.empty:
        return wide
    # Index labels are integer buckets at runtime; cast so max/min type-check.
    lo = max(cast(int, wide[c].first_valid_index()) for c in wide.columns)
    hi = min(cast(int, wide[c].last_valid_index()) for c in wide.columns)
    grid = range(int(wide.index.min()), int(wide.index.max()) + 1)
    return wide.reindex(grid).ffill().loc[lo:hi]


def basis_bps(
    aligned: pd.DataFrame, base: str = BASE_VENUE, quote: str = QUOTE_VENUE
) -> pd.Series:
    """Basis in basis points: ``(quote_mid - base_mid) / base_mid * 1e4``.

    Positive means the quote venue (Coinbase, USD) is richer than the base
    (Binance, USDT) -- a USD premium / USDT discount.
    """
    return (aligned[quote] - aligned[base]) / aligned[base] * 1e4


def basis_summary(basis: pd.Series) -> dict[str, float]:
    """Compact distribution of a basis series: level, dispersion, extremes, sign."""
    b = basis.dropna()
    if b.empty:
        return {"n": 0}
    return {
        "n": int(b.size),
        "mean_bps": float(b.mean()),
        "std_bps": float(b.std()),
        "min_bps": float(b.min()),
        "max_bps": float(b.max()),
        "pct_quote_rich": float((b > 0).mean()),  # share of time quote venue is richer
    }


def lead_lag(
    aligned: pd.DataFrame,
    base: str = BASE_VENUE,
    quote: str = QUOTE_VENUE,
    max_lag: int = 10,
) -> pd.Series:
    """Cross-correlation of bucketed mid *returns* across lags, base vs quote.

    Entry at lag ``k`` is ``corr(base_ret[t], quote_ret[t + k])``. A positive-``k``
    peak means the base venue's move at ``t`` predicts the quote venue's move ``k``
    buckets later -- the base *leads*; a negative-``k`` peak means the quote leads.
    Indexed by lag (in buckets), so :func:`peak_lag` reads the winner off it.
    """
    base_ret = np.log(aligned[base]).diff()
    quote_ret = np.log(aligned[quote]).diff()
    lags = range(-max_lag, max_lag + 1)
    return pd.Series({k: base_ret.corr(quote_ret.shift(-k)) for k in lags}, name="corr")


def peak_lag(corr: pd.Series) -> tuple[int, float]:
    """The lag of maximum cross-correlation and its value ``(lag, corr)``."""
    k = int(corr.idxmax())
    return k, float(corr.loc[k])


def cross_venue_report(
    symbol: str = "BTCUSDT",
    day: str = "",
    bucket_ms: int = 1000,
    max_lag: int = 10,
    query: Callable[[str], pd.DataFrame] | None = None,
) -> dict[str, object]:
    """Load, align, and summarize the cross-venue basis and lead-lag for a day.

    Returns a dict of the two headline signals plus context (overlap length,
    bucket size), suitable for logging, an alert, or a notebook table.
    """
    day = day or _yesterday_utc()
    wide = load_mids(symbol, day, bucket_ms, query=query)
    aligned = align(wide)
    if aligned.empty or len(aligned.columns) < 2:
        return {"symbol": symbol, "day": day, "buckets": 0, "note": "insufficient overlap"}

    basis = basis_bps(aligned)
    corr = lead_lag(aligned, max_lag=max_lag)
    lag, lag_corr = peak_lag(corr)
    leader = BASE_VENUE if lag > 0 else QUOTE_VENUE if lag < 0 else "simultaneous"
    return {
        "symbol": symbol,
        "day": day,
        "bucket_ms": bucket_ms,
        "buckets": int(len(aligned)),
        "basis": basis_summary(basis),
        "lead_lag": {
            "leader": leader,
            "lag_buckets": lag,
            "lag_ms": lag * bucket_ms,
            "corr_at_peak": round(lag_corr, 4),
        },
    }


def _yesterday_utc() -> str:
    return (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).date().isoformat()


def _format_report(r: dict[str, object]) -> str:
    """Render a report dict as a short human-readable block."""
    if not r.get("buckets"):
        return f"{r['symbol']} {r['day']}: {r.get('note', 'no data')}"
    b = cast("dict[str, float]", r["basis"])
    ll = cast("dict[str, object]", r["lead_lag"])
    return "\n".join(
        [
            f"Cross-venue {r['symbol']}  {r['day']}  "
            f"({r['buckets']} x {r['bucket_ms']}ms buckets, {BASE_VENUE} vs {QUOTE_VENUE})",
            f"  basis  mean {b['mean_bps']:+.2f} bps  std {b['std_bps']:.2f}  "
            f"[{b['min_bps']:+.1f}, {b['max_bps']:+.1f}]  "
            f"{QUOTE_VENUE} richer {b['pct_quote_rich']:.0%} of the time",
            f"  lead-lag  {ll['leader']} leads by {abs(cast(int, ll['lag_ms']))}ms  "
            f"(peak corr {ll['corr_at_peak']} at lag {ll['lag_buckets']})",
        ]
    )


if __name__ == "__main__":
    _symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    _day = sys.argv[2] if len(sys.argv) > 2 else ""
    print(_format_report(cross_venue_report(_symbol, _day)))
