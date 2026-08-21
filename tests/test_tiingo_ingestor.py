"""Tests for the Tiingo end-of-day ingestor.

Tiingo returns raw and adjusted prices side by side, which is exactly the shape that
invites a silent mistake: both sets are plausible, both are internally coherent, and
picking the wrong one produces data that fails no structural check.
"""


from qde.ingest.tiingo import TiingoIngestor
from qde.registry import get_spec


def _record(**over):
    base = {
        "date": "2025-12-05T00:00:00.000Z",
        "open": 88.0, "high": 89.0, "low": 87.0, "close": 88.47, "volume": 1000,
        # A 2:1 split day: the adjusted series is continuous, the raw one halves.
        "adjOpen": 44.0, "adjHigh": 44.5, "adjLow": 43.5, "adjClose": 44.235,
        "adjVolume": 2000, "divCash": 0.0, "splitFactor": 2.0,
    }
    base.update(over)
    return base


def _normalize(records):
    return TiingoIngestor(get_spec("tiingo")).normalize(records)


def test_the_adjusted_series_is_stored_not_the_raw_one():
    """Raw prices make every split look like a crash.

    XLB read 88.47 -> 44.09 on 2025-12-05 — a clean -50% return that never happened.
    Twelve of twenty-seven symbols carried such artifacts, and they flow straight into
    returns, ATR and realized vol in the gold marts.
    """
    out = _normalize([_record()])
    assert float(out["close"].iloc[0]) == 44.235, "must be adjClose, not close"
    assert float(out["open"].iloc[0]) == 44.0
    assert float(out["volume"].iloc[0]) == 2000, "adjVolume tracks the split too"


def test_conventions_are_never_mixed():
    # An adjusted close beside a raw high gives high < close — the exact incoherence
    # qde.verify exists to catch, produced by a one-line mistake.
    from qde.verify import verify_frame

    out = _normalize([_record()])
    assert verify_frame(out, "bars", "tiingo", "XLB") == []
    assert float(out["high"].iloc[0]) >= float(out["close"].iloc[0])


def test_a_split_does_not_appear_as_a_price_move():
    before = _record(date="2025-12-04T00:00:00.000Z", close=88.0, adjClose=44.0)
    after = _record(date="2025-12-05T00:00:00.000Z", close=44.09, adjClose=44.09)

    out = _normalize([before, after])
    move = float(out["close"].pct_change().dropna().iloc[0])
    assert abs(move) < 0.05, f"a 2:1 split must not read as a {move:.0%} return"


def test_the_index_is_utc_normalised():
    out = _normalize([_record()])
    assert out.index.tz is not None
    assert str(out.index[0].date()) == "2025-12-05"
    assert out.index[0].hour == 0


def test_an_empty_response_returns_an_empty_frame():
    assert _normalize([]).empty
