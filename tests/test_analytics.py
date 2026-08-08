"""Tests for the cross-venue microstructure analytics (offline, synthetic).

The DuckDB/R2 loading is validated live; here the pure math is pinned: alignment
over a ragged overlap, the basis, and that lead-lag recovers a known lead.
"""

import numpy as np
import pandas as pd

from qde.analytics import (
    align,
    basis_bps,
    basis_summary,
    cross_venue_report,
    lead_lag,
    load_mids,
    peak_lag,
)


def test_align_restricts_to_overlap_and_fills_gaps():
    # binance spans buckets 0..6, coinbase 2..8; the overlap is 2..6. Missing
    # buckets forward-fill the prevailing mid onto a gap-free grid.
    wide = pd.DataFrame(
        {
            "binance": {0: 100.0, 3: 101.0, 6: 102.0},
            "coinbase": {2: 200.0, 5: 201.0, 8: 202.0},
        }
    )

    out = align(wide)

    assert list(out.index) == [2, 3, 4, 5, 6]  # overlap only, gap-free
    assert not out.isna().any().any()  # gaps filled
    assert out.loc[4, "binance"] == 101.0  # ffill from bucket 3
    assert out.loc[4, "coinbase"] == 200.0  # ffill from bucket 2


def test_basis_bps_and_summary():
    aligned = pd.DataFrame({"binance": [100.0, 100.0], "coinbase": [100.5, 99.5]})
    b = basis_bps(aligned)
    assert list(np.round(b, 2)) == [50.0, -50.0]  # +/- 0.5% -> +/- 50 bps

    s = basis_summary(b)
    assert s["n"] == 2
    assert round(s["mean_bps"], 6) == 0.0
    assert s["min_bps"] == -50.0 and s["max_bps"] == 50.0
    assert s["pct_quote_rich"] == 0.5


def _leading_frame(lead=2, n=60):
    """A wide frame where coinbase mirrors binance shifted forward by `lead`."""
    rng = np.random.default_rng(0)
    base = 100.0 + np.cumsum(rng.normal(0, 0.5, n))
    quote = np.concatenate([np.full(lead, base[0]), base[:-lead]])  # quote[t] = base[t-lead]
    return pd.DataFrame({"binance": base, "coinbase": quote}, index=range(n))


def test_lead_lag_recovers_a_known_lead():
    aligned = _leading_frame(lead=2)
    corr = lead_lag(aligned, max_lag=5)
    lag, value = peak_lag(corr)
    assert lag == 2  # binance moves first; coinbase follows 2 buckets later
    assert value > 0.99  # a clean lead is near-perfectly correlated at that lag


def test_cross_venue_report_end_to_end_with_injected_query():
    # Feed load_mids a fake lake.query returning the long (source, bucket, mid)
    # shape, so the whole report path runs with no R2.
    frame = _leading_frame(lead=2)
    long = frame.reset_index(names="bucket").melt(
        id_vars="bucket", var_name="source", value_name="mid"
    )

    r = cross_venue_report(bucket_ms=250, max_lag=5, query=lambda _sql: long)

    assert r["buckets"] > 0
    assert r["lead_lag"]["leader"] == "binance"
    assert r["lead_lag"]["lag_buckets"] == 2
    assert r["lead_lag"]["lag_ms"] == 500  # 2 buckets x 250ms
    assert "mean_bps" in r["basis"]


def test_load_mids_empty_is_safe():
    empty = pd.DataFrame(columns=["source", "bucket", "mid"])
    out = load_mids(query=lambda _sql: empty)
    assert out.empty
