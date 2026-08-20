"""Tests for the pre-storage frame contract.

These are the checks a generated ingestor has to pass before its output is
trusted. Each one below corresponds to a way an ingestor drafted from API
documentation goes wrong *without raising* — the frame arrives complete,
correctly typed, and plausible, and is simply not the data anyone asked for.
"""

import pandas as pd
import pytest

from qde.verify import (
    _PROXY_RECENT_DAYS,
    cross_check,
    proxy_check,
    self_consistency,
    series_self_consistency,
    verify_frame,
)


def _bars(**overrides) -> pd.DataFrame:
    idx = pd.DatetimeIndex(
        pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"], utc=True), name="date"
    )
    frame = pd.DataFrame(
        {
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 11.0],
            "close": [10.5, 11.5, 12.5],
            "volume": [100.0, 110.0, 120.0],
        },
        index=idx,
    )
    for column, values in overrides.items():
        frame[column] = values
    return frame


def _checks(frame, group="bars") -> set[str]:
    return {v.check for v in verify_frame(frame, group, "tiingo", "SPY")}


def test_a_well_formed_frame_passes():
    assert verify_frame(_bars(), "bars") == []


# --- the mismapping case, which is the whole point -------------------------------


def test_a_mismapped_price_column_is_caught():
    # `adjClose` mapped onto `close` is the canonical generated-ingestor bug: every
    # field present, every type right, every number plausible. A human reviewing
    # `df["close"]` nods; `high >= close` does not.
    assert "ohlc_coherent" in _checks(_bars(close=[99.0, 11.5, 12.5]))


def test_floating_point_noise_is_not_a_defect():
    # yfinance's dividend adjustment leaves ~1e-16 relative noise, which a strict
    # comparison reports as incoherent. A real defect is off by 1e-4 or worse.
    frame = _bars()
    frame["close"] = frame["high"] * (1 + 1e-15)
    assert "ohlc_coherent" not in _checks(frame)


def test_a_low_above_the_open_is_caught():
    assert "ohlc_coherent" in _checks(_bars(low=[10.5, 10.0, 11.0]))


# --- shape ------------------------------------------------------------------------


def test_a_timezone_naive_index_is_an_error():
    # pandas reads a naive index as local time. Storing one shifts every timestamp
    # by the reader's offset, with nothing raised anywhere.
    frame = _bars()
    frame.index = frame.index.tz_localize(None)
    assert "index" in _checks(frame)


def test_duplicate_index_values_are_caught_before_the_upsert_eats_them():
    frame = _bars()
    frame.index = pd.DatetimeIndex(
        pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-03"], utc=True), name="date"
    )
    assert "index" in _checks(frame)


def test_a_missing_column_is_named():
    violations = verify_frame(_bars().drop(columns=["volume"]), "bars")
    assert len(violations) == 1
    assert "volume" in violations[0].detail


def test_structural_failure_suppresses_the_cascade():
    # A frame with a broken index fails every downstream check too, and those
    # follow-on violations bury the finding that actually matters.
    frame = _bars(close=[99.0, 11.5, 12.5])
    frame.index = pd.RangeIndex(len(frame))
    assert _checks(frame) == {"index"}


def test_an_empty_frame_says_what_should_have_happened_instead():
    violations = verify_frame(_bars().iloc[0:0], "bars")
    assert [v.check for v in violations] == ["empty"]
    assert "NoNewData" in violations[0].detail


# --- plausibility (warnings — markets do surprise you) ----------------------------


def test_a_constant_price_reads_as_the_wrong_field():
    flat = _bars(open=[10.5] * 3, high=[10.5] * 3, low=[10.5] * 3, close=[10.5] * 3)
    assert "variance" in _checks(flat)


def test_a_decimal_shift_shows_up_as_an_impossible_move():
    assert "returns" in _checks(_bars(close=[10.5, 11.5, 12500.0], high=[11.0, 12.0, 13000.0]))


def test_a_violent_but_real_move_is_not_flagged():
    # -60% in a day is a bad day in crypto, not a units error. The threshold has to
    # sit above anything a real market reaches, or the check cries wolf.
    assert "returns" not in _checks(_bars(close=[10.5, 11.5, 4.6], low=[4.0, 4.0, 4.0]))


def test_negative_volume_is_an_error():
    assert "volume" in _checks(_bars(volume=[100.0, -5.0, 120.0]))


# --- other groups -----------------------------------------------------------------


def test_events_must_not_be_observed_before_they_were_scheduled():
    frame = pd.DataFrame(
        {
            "event_id": ["CPI:2024-01-01"],
            "revision_seq": [0],
            "scheduled_ts": pd.to_datetime(["2024-02-13"], utc=True),
            "observed_ts": pd.to_datetime(["2024-02-01"], utc=True),  # before release
        }
    )
    assert "bitemporal" in _checks(frame, group="events")


def test_a_series_frame_may_name_its_column_anything():
    # This test previously asserted the opposite — that a column not called `value` was
    # a defect — and that assumption is what rejected binancefut and CFTC at intake in
    # production. Any column name is a metric name; `upsert_series_frame` stores it
    # under `metric=<name>`.
    idx = pd.DatetimeIndex(pd.to_datetime(["2024-01-01"], utc=True), name="date")
    assert verify_frame(pd.DataFrame({"amount": [1.0]}, index=idx), "series", "s", "X") == []


@pytest.mark.parametrize("group", ["bars", "events"])
def test_fixed_shape_groups_declare_their_required_columns(group):
    # A group with no required columns would silently accept anything.
    from qde.verify import _REQUIRED_COLUMNS

    assert _REQUIRED_COLUMNS[group]


def test_the_series_contract_is_enforced_even_though_it_names_no_columns():
    # `series` cannot list required columns — it has two legal shapes — so the check
    # that it is still *checked* has to be behavioural. Asserting the tuple was
    # non-empty is what made naming "value" look safe.
    from qde.verify import _REQUIRED_COLUMNS

    assert _REQUIRED_COLUMNS["series"] == ()
    idx = pd.DatetimeIndex(pd.date_range("2024-01-01", periods=3, freq="D", tz="UTC"))
    assert verify_frame(pd.DataFrame(index=idx), "series", "s", "X"), "empty must fail"
    assert verify_frame(
        pd.DataFrame({"m": ["x", "y", "z"]}, index=idx), "series", "s", "X"
    ), "unparseable must fail"


# --- epoch-unit errors: a perfectly valid index that is entirely wrong ------------
#
# Reading milliseconds as seconds throws every date into the year 55000; reading
# seconds as milliseconds collapses them into January 1970. Both produce a clean,
# sorted, timezone-aware DatetimeIndex — they are the reason `start`/`end` matter.


def _at(index) -> pd.DataFrame:
    return _bars().set_axis(pd.DatetimeIndex(index, name="date"))


def test_milliseconds_read_as_seconds_lands_in_the_far_future():
    ms = [1704067200000, 1704153600000, 1704240000000]
    frame = _at(pd.to_datetime(ms, unit="s", utc=True))
    assert "future_dates" in _checks(frame)


def test_seconds_read_as_milliseconds_lands_in_1970():
    # 1970 is not implausible on its own — GDP data really does go back to 1947 —
    # so only the requested range reveals this one.
    secs = [1704067200, 1704153600, 1704240000]
    frame = _at(pd.to_datetime(secs, unit="ms", utc=True))
    assert verify_frame(frame, "bars") == []  # invisible without the range
    violations = verify_frame(frame, "bars", start="2024-01-01", end="2024-01-03")
    assert "range" in {v.check for v in violations}


def test_data_outside_the_requested_window_is_flagged():
    # An ingestor that ignores its date parameters and returns full history.
    assert "range" in {
        v.check for v in verify_frame(_bars(), "bars", start="2030-01-01", end="2030-12-31")
    }


def test_a_clean_frame_passes_with_every_parameter_supplied():
    assert verify_frame(_bars(), "bars", start="2024-01-01", end="2024-01-03", interval="1d") == []


def test_the_wrong_resolution_is_caught():
    # Hourly candles satisfy every other check when daily was requested — the
    # numbers are real, they are just the wrong series.
    hourly = _at(pd.to_datetime(
        ["2024-01-01T00:00", "2024-01-01T01:00", "2024-01-01T02:00"], utc=True
    ))
    assert "interval" in {v.check for v in verify_frame(hourly, "bars", interval="1d")}


def test_weekend_gaps_do_not_look_like_the_wrong_resolution():
    # Daily equity bars skip weekends; the modal spacing is still one day.
    weekdays = _at(pd.to_datetime(["2024-01-04", "2024-01-05", "2024-01-08"], utc=True))
    assert "interval" not in {v.check for v in verify_frame(weekdays, "bars", interval="1d")}


# --- unparseable numerics: present, plausible, and NaN the moment you touch them --


def test_prices_that_do_not_parse_are_caught_not_silently_coerced():
    # Every numeric check coerces with errors="coerce", and NaN compares false
    # against everything — so a thousands separator sails through OHLC coherence,
    # variance and returns without a single violation.
    commas = _bars(
        open=["1,010"] * 3, high=["1,011"] * 3, low=["1,009"] * 3, close=["1,010"] * 3
    )
    checks = _checks(commas)
    assert "numeric" in checks
    assert "ohlc_coherent" not in checks, "should report the parse failure, not compare NaNs"


def test_a_null_price_is_a_defect_not_a_gap():
    assert "nulls" in _checks(_bars(close=[10.5, None, 12.5]))


def test_an_entirely_null_price_column_is_caught():
    assert "nulls" in _checks(_bars(close=[None, None, None]))


# --- events keys ------------------------------------------------------------------


def _events(event_ids, seqs) -> pd.DataFrame:
    n = len(event_ids)
    return pd.DataFrame({
        "event_id": event_ids,
        "revision_seq": seqs,
        "scheduled_ts": pd.to_datetime(["2024-01-01"] * n, utc=True),
        "observed_ts": pd.to_datetime(["2024-02-01"] * n, utc=True),
    })


def test_duplicate_event_keys_are_caught():
    # Events are keyed by columns, not the index, so the index duplicate check
    # cannot see this one.
    frame = _events(["CPI:2024-01", "CPI:2024-01"], [0, 0])
    assert "key" in _checks(frame, group="events")


def test_a_hole_in_the_revision_sequence_is_caught():
    # A missing vintage defeats the entire point of the bitemporal group.
    frame = _events(["CPI:2024-01"] * 3, [0, 1, 3])
    assert "revision_seq" in _checks(frame, group="events")


def test_a_contiguous_revision_sequence_passes():
    assert verify_frame(_events(["CPI:2024-01"] * 3, [0, 1, 2]), "events") == []


# --- wiring: recorded, never enforced ---------------------------------------------


def test_a_suspect_frame_is_still_stored(tmp_path, monkeypatch):
    """The load-bearing property of the whole design.

    Bronze is the replay log. Rejecting a suspect frame would destroy the evidence
    needed to diagnose it — and if the check turns out to be miscalibrated, the
    data it refused is simply gone. So the frame lands intact and the violation
    records the doubt.
    """
    import qde.storage as storage_mod
    from qde.storage import _bars_path, save_ohlcv, update_ohlcv

    seed = _bars()
    save_ohlcv_frame = seed.iloc[:1]
    monkeypatch.setattr(storage_mod, "load_ohlcv", lambda *a, **k: save_ohlcv_frame)
    save_ohlcv("BTCUSDT", source="binance", start="2024-01-01", base_dir=str(tmp_path))

    # Now return a frame with a mismapped price column.
    broken = _bars(close=[10.5, 99.0, 12.5]).iloc[1:]
    monkeypatch.setattr(storage_mod, "load_ohlcv", lambda *a, **k: broken)

    intake: list = []
    update_ohlcv("BTCUSDT", source="binance", base_dir=str(tmp_path), violations=intake)

    assert any(v.check == "ohlc_coherent" for v in intake), "the defect must be recorded"

    stored = pd.read_parquet(_bars_path("BTCUSDT", "binance", "1d", str(tmp_path)))
    assert len(stored) == 3, "the frame must be stored anyway — bronze keeps everything"
    assert 99.0 in set(stored["close"]), "stored byte-for-byte, not sanitised"


def test_callers_that_do_not_ask_for_violations_are_unaffected(tmp_path, monkeypatch):
    # The parameter is optional precisely so every existing caller is untouched.
    import qde.storage as storage_mod
    from qde.storage import save_ohlcv, update_ohlcv

    monkeypatch.setattr(storage_mod, "load_ohlcv", lambda *a, **k: _bars().iloc[:1])
    save_ohlcv("BTCUSDT", source="binance", start="2024-01-01", base_dir=str(tmp_path))
    broken = _bars(close=[10.5, 99.0, 12.5]).iloc[1:]
    monkeypatch.setattr(storage_mod, "load_ohlcv", lambda *a, **k: broken)

    update_ohlcv("BTCUSDT", source="binance", base_dir=str(tmp_path))  # no violations list


# --- tier 3: the second opinion ---------------------------------------------------
#
# Every check above compares a frame to itself, and all of them pass for a source
# that returns confidently wrong numbers. These are the failures only an
# independent feed can reveal. The loader is injected throughout — the suite must
# run offline (US CI runners are geo-blocked from Binance).


def _series(values, start="2024-01-01") -> pd.DataFrame:
    idx = pd.DatetimeIndex(pd.date_range(start, periods=len(values), freq="D", tz="UTC"),
                           name="date")
    return pd.DataFrame(
        {"open": values, "high": [v * 1.01 for v in values],
         "low": [v * 0.99 for v in values], "close": values,
         "volume": [1.0] * len(values)},
        index=idx,
    )


_TRUTH = [100.0, 103.0, 99.0, 105.0, 108.0, 104.0, 110.0, 107.0]


def _peer(_symbol, start=None, end=None, interval=None, source=None):
    return _series(_TRUTH)


def _cross(frame, **kw):
    return cross_check(frame, "BTCUSDT", "binance", peers=["bybit"], loader=_peer, **kw)


def test_an_honest_frame_agrees_with_its_peer():
    assert _cross(_series(_TRUTH)) == []


def test_a_small_basis_between_venues_is_not_a_defect():
    # A USDT pair against a USD one carries a real premium. Flagging it would be
    # policing the market, not the data.
    assert _cross(_series([v * 1.0007 for v in _TRUTH])) == []


def test_a_units_error_is_caught():
    # Prices in cents. Internally perfect — every ratio preserved — so nothing
    # before this tier can see it.
    violations = _cross(_series([v * 100 for v in _TRUTH]))
    assert violations and violations[0].check == "cross_source"
    assert violations[0].severity == "error"
    # Only one peer was supplied, so there is nothing to adjudicate with — the
    # verdict is still an error, but honest that it rests on a single opinion.
    assert "no other source could adjudicate" in violations[0].detail


def test_a_stale_feed_is_caught_by_movement_not_level():
    # The case that first slipped through: last month's prices sat inside the 5%
    # level tolerance because the asset had not moved much. What a frozen feed
    # cannot fake is the day-to-day movement.
    frozen = [100.0, 100.4, 100.1, 100.6, 100.2, 100.8, 100.3, 100.9]
    violations = _cross(_series(frozen))
    assert violations, "a stale feed inside the level tolerance must still fail"
    assert "correlation" in violations[0].detail


def test_reordered_dates_break_the_correlation():
    shuffled = [107.0, 99.0, 110.0, 100.0, 104.0, 108.0, 103.0, 105.0]
    assert _cross(_series(shuffled))


def test_correlation_is_skipped_when_there_is_too_little_to_correlate():
    # A correlation over three points is noise; reporting it would be worse than
    # reporting nothing.
    short = _TRUTH[:3]

    def peer(_s, start=None, end=None, interval=None, source=None):
        return _series(short)

    assert cross_check(_series(short), "BTCUSDT", "binance", peers=["bybit"], loader=peer) == []


# --- unverifiable must never look like verified -----------------------------------


def test_a_symbol_with_no_peer_is_reported_as_unverifiable():
    # Every equity in this lake has exactly one source. "Nothing disagreed" and
    # "nothing could disagree" are different states.
    violations = cross_check(_series(_TRUTH), "SPY", "yfinance", peers=[], loader=_peer)
    assert len(violations) == 1
    assert violations[0].severity == "warn"
    assert "unverifiable" in violations[0].detail


def test_an_unreachable_peer_is_reported_not_treated_as_agreement():
    def down(*_a, **_k):
        raise ConnectionError("peer is down")

    violations = cross_check(_series(_TRUTH), "BTCUSDT", "binance", peers=["bybit"], loader=down)
    assert len(violations) == 1
    assert violations[0].severity == "warn"
    assert "ConnectionError" in violations[0].detail


def test_no_overlapping_dates_is_not_silent_agreement():
    def elsewhere(*_a, **_k):
        return _series(_TRUTH, start="2020-01-01")

    violations = cross_check(
        _series(_TRUTH), "BTCUSDT", "binance", peers=["bybit"], loader=elsewhere
    )
    assert violations and "no overlapping dates" in violations[0].detail


def test_one_agreeing_peer_ends_the_search():
    # Polling further peers costs requests and cannot strengthen a verdict that
    # already rests on an independent feed.
    calls: list[str] = []

    def counting(_symbol, start=None, end=None, interval=None, source=None):
        calls.append(source)
        return _series(_TRUTH)

    cross_check(_series(_TRUTH), "BTCUSDT", "binance",
                peers=["bybit", "okx", "kucoin"], loader=counting)
    assert calls == ["bybit"]


# --- adjudication: disagreement names a conflict, not a culprit -------------------


def _panel(**by_source):
    """A loader serving a different frame per source."""
    def loader(_symbol, start=None, end=None, interval=None, source=None):
        return by_source[source]
    return loader


def test_two_peers_agreeing_against_us_convicts_our_frame():
    ours = _series([v * 100 for v in _TRUTH])
    loader = _panel(bybit=_series(_TRUTH), okx=_series(_TRUTH))
    violations = cross_check(ours, "BTCUSDT", "binance", peers=["bybit", "okx"], loader=loader)
    assert violations[0].severity == "error"
    assert "two independent sources agree against this frame" in violations[0].detail


def test_a_broken_peer_does_not_convict_a_good_frame():
    # The false-accusation case: our data is fine and the peer polled first is the
    # broken one. Reporting on one opinion would blame the wrong frame.
    ours = _series(_TRUTH)
    loader = _panel(bybit=_series([v * 100 for v in _TRUTH]), okx=_series(_TRUTH))
    violations = cross_check(ours, "BTCUSDT", "binance", peers=["bybit", "okx"], loader=loader)
    assert violations[0].severity == "warn", "our frame must not be convicted"
    assert "bybit is the likely outlier" in violations[0].detail


def test_a_source_dropping_days_is_caught_even_though_its_rows_are_perfect():
    # Every row it returns is impeccable; comparing only the overlap is exactly
    # how the missing two-thirds stays invisible. Two independent peers are unlikely
    # to be short in the same way, so agreement between them convicts.
    thin = _series(_TRUTH).iloc[:2]
    violations = cross_check(
        thin, "BTCUSDT", "binance", peers=["bybit", "okx"], loader=_peer
    )
    assert violations[0].severity == "error"
    assert "dropping data" in violations[0].detail


def test_one_peer_alone_cannot_convict_us_of_dropping_data():
    # A peer that answers a daily request with hourly candles has 24x our rows.
    # Reporting on its word alone convicts a correct frame — the same false
    # accusation the level check adjudicates a third source to avoid.
    thin = _series(_TRUTH).iloc[:2]
    violations = cross_check(thin, "BTCUSDT", "binance", peers=["bybit"], loader=_peer)
    assert violations[0].severity == "warn"
    assert "does not say which of the two is wrong" in violations[0].detail


def test_the_correlation_threshold_matches_measured_reality():
    # Across every venue pair in this lake over 382 days the worst observed return
    # correlation was 0.9983. A threshold below that is slack; far below it is
    # decoration.
    from qde.verify import _MIN_RETURN_CORRELATION

    assert 0.8 <= _MIN_RETURN_CORRELATION < 0.9983


# --- self-consistency: the only check needing no second source --------------------
#
# Rests on a property every honest feed has and no broken one does: settled history
# does not change. This is what protects a source with no peer, where every other
# tier goes quiet.


def _yesterdays(values):
    """A frame ending before today, so nothing is a still-forming bar."""
    end = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=2)
    idx = pd.DatetimeIndex(
        pd.date_range(end=end, periods=len(values), freq="D", tz="UTC"), name="date"
    )
    return _series(values).set_axis(idx)


def test_a_source_that_reproduces_its_history_passes():
    stored = _yesterdays(_TRUTH)
    assert self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: stored) == []


def test_a_source_that_revises_settled_history_is_caught():
    stored = _yesterdays(_TRUTH)
    revised = _yesterdays([v * 1.02 if i == 3 else v for i, v in enumerate(_TRUTH)])
    violations = self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: revised)
    assert violations and violations[0].severity == "error"
    assert "revised history" in violations[0].detail


def test_a_uniform_restatement_reads_as_a_corporate_action():
    # A split or dividend rescales the whole series by one ratio. That is expected
    # bookkeeping rather than corruption — but it still invalidates any backtest
    # run on the old numbers, so it is reported, at warn.
    stored = _yesterdays(_TRUTH)
    split = _yesterdays([v / 2 for v in _TRUTH])
    violations = self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: split)
    assert violations and violations[0].severity == "warn"
    assert "corporate action" in violations[0].detail


def test_history_that_disappears_on_re_fetch_is_caught():
    # A source quietly dropping history is invisible to anything that only reads
    # what it currently returns.
    stored = _yesterdays(_TRUTH)
    shorter = stored.iloc[2:]
    violations = self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: shorter)
    assert any("dropping history" in v.detail for v in violations)


def test_rounding_noise_is_not_a_revision():
    stored = _yesterdays(_TRUTH)
    noisy = _yesterdays([v * (1 + 1e-12) for v in _TRUTH])
    assert self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: noisy) == []


def test_an_unreachable_re_fetch_is_reported_as_unverified():
    def down(*_a, **_k):
        raise TimeoutError("no answer")

    violations = self_consistency(_yesterdays(_TRUTH), "SPY", "tiingo", loader=down)
    assert violations[0].severity == "warn"
    assert "unverified" in violations[0].detail


# --- verification status: what is knowable, published ------------------------------


def test_a_symbol_with_peers_is_corroborated():
    from qde.verify import verification_status

    status = verification_status("BTCUSDT", "binance")
    assert status["level"] == "corroborated"
    assert len(status["peers"]) >= 5


def test_a_single_source_symbol_is_not_dressed_up_as_verified():
    # Every equity in this lake has exactly one source. Reporting it the same way
    # as a six-venue crypto series is the flattery this field exists to prevent.
    from qde.verify import verification_status

    status = verification_status("SPY", "yfinance")
    assert status["level"] == "proxy_only"
    assert status["peers"] == []
    assert "not corroborated" in status["basis"]


def test_a_source_is_only_as_trusted_as_its_weakest_series(tmp_path, monkeypatch):
    # A source holding one corroborated symbol and one peerless one is only as trusted
    # as the peerless one. Built against an explicit lake rather than the real ./data,
    # so the assertion means the same thing on a CI runner with no lake at all.
    from qde.catalogue import _verification_summary

    monkeypatch.setattr(
        "qde.registry.declared_series",
        lambda group=None: [
            ("acme", "SHARED", "1d"),
            ("other", "SHARED", "1d"),  # a real peer for SHARED
            ("acme", "ALONE", "1d"),  # nobody else carries this
        ],
    )
    monkeypatch.setattr(
        "qde.storage.list_bars_series",
        lambda base_dir: pd.DataFrame(
            [
                {"source": "acme", "symbol": "SHARED", "interval": "1d"},
                {"source": "other", "symbol": "SHARED", "interval": "1d"},
                {"source": "acme", "symbol": "ALONE", "interval": "1d"},
            ]
        ),
    )

    summary = _verification_summary("acme", "SHARED, ALONE", str(tmp_path))
    assert summary["by_level"]["corroborated"] == 1, "SHARED has a peer that holds data"
    assert summary["level"] != "corroborated", "must not round up to its best symbol"


def test_the_summary_grades_only_symbols_the_source_actually_stores(tmp_path, monkeypatch):
    # `symbols` comes from the registry, which states intent. yfinance declares BTCUSDT
    # and holds none, and grading it reported "3 corroborated" for rows that do not
    # exist — a verification figure describing absent data.
    from qde.catalogue import _verification_summary

    monkeypatch.setattr(
        "qde.registry.declared_series",
        lambda group=None: [("acme", "REAL", "1d"), ("peer", "GHOST", "1d")],
    )
    monkeypatch.setattr(
        "qde.storage.list_bars_series",
        lambda base_dir: pd.DataFrame(
            [
                {"source": "acme", "symbol": "REAL", "interval": "1d"},
                {"source": "peer", "symbol": "GHOST", "interval": "1d"},
            ]
        ),
    )

    summary = _verification_summary("acme", "REAL, GHOST", str(tmp_path))
    assert sum(summary["by_level"].values()) == 1, "GHOST is declared but not stored here"


# --- self-consistency covers every price column, not just close -------------------


def test_a_revision_to_high_is_caught_even_when_close_is_untouched():
    # Checking one column while trusting four is a spot check reporting as a
    # guarantee — a source is equally capable of revising a high.
    stored = _yesterdays(_TRUTH)
    revised = stored.copy()
    revised.iloc[1, revised.columns.get_loc("high")] *= 1.05
    violations = self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: revised)
    assert violations and violations[0].severity == "error"
    assert "'high'" in violations[0].detail


def test_a_uniform_restatement_reads_as_a_corporate_action_not_corruption():
    # Every row moving by the same ratio is a split or dividend rewriting an
    # adjusted series — expected bookkeeping. Reporting it at the same severity
    # as a broken feed would train the reader to ignore both. It still warns:
    # a legitimate restatement invalidates an old backtest just as thoroughly.
    stored = _yesterdays(_TRUTH)
    adjusted = stored.copy()
    for column in ("open", "high", "low", "close"):
        adjusted[column] = adjusted[column] / 2
    violations = self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: adjusted)
    assert violations and violations[0].severity == "warn"
    assert "corporate action" in violations[0].detail


def test_a_revision_to_open_is_caught():
    stored = _yesterdays(_TRUTH)
    revised = stored.copy()
    revised.iloc[0, revised.columns.get_loc("open")] = 999.0
    assert self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: revised)


def test_volume_restatement_is_a_warning_not_an_error():
    # Exchanges restate volume as late prints settle, far more readily than they
    # restate a price. Folding it in with prices would make routine bookkeeping
    # look like a broken feed.
    stored = _yesterdays(_TRUTH)
    revised = stored.copy()
    revised["volume"] = revised["volume"] * 10
    violations = self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: revised)
    assert violations and all(v.severity == "warn" for v in violations)
    assert "volume" in violations[0].detail


def test_back_filled_history_is_surfaced():
    # A series that grows retroactively means any backtest run before the fill saw
    # a different history than the one now on disk.
    full = _yesterdays(_TRUTH)
    with_gap = full.drop(full.index[2])
    violations = self_consistency(with_gap, "SPY", "tiingo", loader=lambda *a, **k: full)
    assert violations and violations[0].severity == "warn"
    assert "back-filled" in violations[0].detail


def test_a_frame_without_a_date_index_is_skipped_not_crashed():
    frame = _yesterdays(_TRUTH).reset_index(drop=True)
    assert self_consistency(frame, "SPY", "tiingo", loader=lambda *a, **k: frame) == []


# --- the status must not claim evidence it does not have --------------------------


def test_unrelated_symbols_are_candidates_not_proxies():
    # SPY/QQQ correlate 0.949; SPY/TLT correlate 0.12. Which candidates are
    # actually related cannot be known from the registry — only by measuring — so
    # the status must not count them as evidence already in hand.
    from qde.verify import verification_status

    status = verification_status("SPY", "yfinance")
    assert "proxies" not in status, "must not imply confirmed proxies"
    assert "BTCUSDT" in status["proxy_candidates"]
    assert "has to be measured, not assumed" in status["basis"]


# --- proxy: the last witness for a series with no peer ----------------------------
#
# Thresholds here are not chosen, they are measured: across all 190 pairs in the
# live lake, related instruments correlate 0.585-0.932 over full history, and a
# healthy 180-day window never fell below +0.474. Every number below sits where
# that measurement put it, so a test passing means the check would behave the same
# way on the real data.


def _walk(n, seed, drift=0.0):
    """A deterministic random-walk price series, so a 'related' pair can be built."""
    import numpy as np

    rng = np.random.default_rng(seed)
    return 100.0 * np.exp(np.cumsum(rng.normal(drift, 0.02, n)))


def _lake(n=900, related=True, seed=7, rho=0.85):
    """Two series with a *known* return correlation, as a fake local loader.

    Built in return space, not price space: the check correlates day-to-day
    movement, and blending two price paths gives no control over that at all — a
    fixture that looked related by construction could easily correlate at 0.3.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    r1 = rng.normal(0, 0.02, n)
    indep = rng.normal(0, 0.02, n)
    r2 = rho * r1 + np.sqrt(1 - rho**2) * indep if related else indep

    base = 100.0 * np.exp(np.cumsum(r1))
    other = 100.0 * np.exp(np.cumsum(r2))
    idx = pd.date_range(end=pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=2),
                        periods=n, freq="D", tz="UTC")

    def frame(values):
        return pd.DataFrame({"open": values, "high": values, "low": values,
                             "close": values, "volume": np.ones(n)}, index=idx)

    store = {"MINE": frame(base), "PEER": frame(other)}

    def loader(symbol, source=None, interval="1d", base_dir="data"):
        return store[symbol]

    return loader, store


def test_a_series_with_a_healthy_proxy_relationship_passes():
    loader, _ = _lake(related=True)
    assert proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader) == []


def _break_window(store, values):
    """Replace exactly the window under test, continuing from the last good price.

    Both details matter. Mutating more than the window puts the corruption into the
    baseline as well, and splicing in a series at a different price level adds one
    enormous return at the join — a single outlier that dominates Pearson
    correlation and destroys the baseline on its own. Either mistake makes the
    check report "no proxy available" instead of "this proxy broke", which is a
    true statement about a fixture rather than a test of the code.
    """
    frame = store["MINE"].copy()
    column = frame.columns.get_loc("close")
    anchor = frame["close"].iloc[-_PROXY_RECENT_DAYS - 1]
    frame.iloc[-_PROXY_RECENT_DAYS:, column] = values / values[0] * anchor
    store["MINE"] = frame


def test_a_frozen_feed_is_an_error_not_a_silent_skip():
    # Zero variance makes the correlation undefined, and NaN is the one value that
    # silently reads as "nothing to report". A frozen feed is the loudest defect
    # this tier exists for and it arrives looking like a missing number.
    import numpy as np

    loader, store = _lake(related=True)
    _break_window(store, np.ones(_PROXY_RECENT_DAYS))
    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations and violations[0].severity == "error"
    assert "frozen" in violations[0].detail


def test_a_feed_that_stops_tracking_its_instrument_is_caught():
    loader, store = _lake(related=True)
    _break_window(store, _walk(_PROXY_RECENT_DAYS, seed=99))
    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations and violations[0].severity == "warn"
    assert "stopped following the market" in violations[0].detail


def test_an_unrelated_candidate_is_never_treated_as_evidence():
    # The whole design rests on this ordering: a check that accepted its own
    # reference would confirm whatever it was pointed at.
    loader, _ = _lake(related=False)
    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations and "correlates with MINE above" in violations[0].detail


def test_the_baseline_never_includes_the_window_it_judges():
    # Measured over history containing the break, a long outage drags the baseline
    # down with it, the pair stops qualifying, and the series is reported as having
    # no proxy rather than a broken one — the defect erasing its own evidence.
    loader, store = _lake(n=900, related=True)
    _break_window(store, _walk(_PROXY_RECENT_DAYS, seed=42))
    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations, "a break inside the window must still be reported as a break"
    assert "no instrument" not in violations[0].detail


def test_too_little_history_is_reported_rather_than_passed():
    loader, _ = _lake(n=200, related=True)
    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations and violations[0].severity == "warn"


def test_an_unreadable_series_is_a_finding_not_a_crash():
    def loader(symbol, source=None, interval="1d", base_dir="data"):
        raise FileNotFoundError(symbol)

    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations and "could not read" in violations[0].detail


def test_a_series_that_stopped_updating_gets_no_proxy_verdict():
    # The window is the last N *rows*, which is the last N days only while the series
    # is alive. A dead series' final 180 rows correlate with its peer exactly as well
    # as they always did, so it passes clean while the verdict describes a period the
    # data does not cover. Confirmed against the real lake: a series dead for 406 days
    # returned no finding at all.
    from qde.verify import PROXY_UNAVAILABLE

    loader, store = _lake(n=900, related=True)
    stale = store["MINE"].copy()
    stale.index = stale.index - pd.Timedelta(days=400)
    store["MINE"] = stale

    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations, "a dead series must not read as verified"
    assert violations[0].check == PROXY_UNAVAILABLE
    assert "no data newer than" in violations[0].detail


def test_no_usable_proxy_is_a_standing_property_not_an_event():
    # GLD and TLT correlate with nothing else in this lake — true this week and every
    # week after. Filed under a separate check so a weekly alert can record it without
    # sending it; alerting weekly on something nobody can fix is how a channel gets
    # muted, and the week it carries a frozen feed gets muted with it.
    from qde.verify import PROXY, PROXY_UNAVAILABLE

    loader, _ = _lake(related=False)
    violations = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert violations[0].check == PROXY_UNAVAILABLE

    loader, store = _lake(related=True)
    _break_window(store, _walk(_PROXY_RECENT_DAYS, seed=5))
    broken = proxy_check("MINE", "s", candidates=[("s", "PEER")], loader=loader)
    assert broken[0].check == PROXY, "an actual break must stay alertable"


def test_a_coverage_complaint_survives_a_later_peer_agreeing_on_price():
    # Peer A has far more rows than us; peer B matches us and agrees on price. The
    # value check is satisfied by B and would return clean, throwing away A's
    # complaint — letting a real gap vanish the moment any one peer matched.
    full, thin = _series(_TRUTH), _series(_TRUTH).iloc[:2]

    def loader(_symbol, start=None, end=None, interval=None, source=None):
        return full if source == "rich" else thin

    violations = cross_check(
        thin, "BTCUSDT", "binance", peers=["rich", "matching"], loader=loader
    )
    assert violations, "the unresolved coverage conflict must not be dropped"
    assert violations[0].severity == "warn"
    assert "unresolved" in violations[0].detail


# --- the group name itself is part of the contract --------------------------------


def test_an_unknown_group_is_refused_rather_than_silently_passed():
    # Every check either skips an unrecognised group or finds nothing in it, so the
    # caller got back an empty list — which in this module means "verified". A typo
    # switched verification off entirely and reported success.
    violations = verify_frame(_bars(), "bar", "tiingo", "SPY")
    assert violations and violations[0].check == "group"
    assert "nothing was verified" in violations[0].detail


@pytest.mark.parametrize("group", ["bars", "series", "events"])
def test_every_real_group_is_still_accepted(group):
    assert "group" not in _checks(_bars(), group=group)


# --- the series group had no parse check at all -----------------------------------


def _series_frame(values, columns=("value",)):
    idx = pd.DatetimeIndex(
        pd.date_range("2024-01-01", periods=len(values), freq="D", tz="UTC"), name="date"
    )
    return pd.DataFrame({c: list(values) for c in columns}, index=idx)


def test_freds_missing_marker_left_unparsed_is_caught():
    # FRED sends a missing observation as the string ".". Its own ingestor coerces
    # correctly — which is the point: this contract exists to hold a *generated*
    # ingestor to what the hand-written one already does.
    violations = verify_frame(_series_frame(["."] * 5), "series", "fred", "UNRATE")
    assert violations and violations[0].check == "numeric"


def test_comma_formatted_numbers_in_a_series_are_caught():
    assert "numeric" in _checks(_series_frame(["1,234.5"] * 5), group="series")


def test_a_window_with_no_observation_at_all_is_surfaced():
    violations = verify_frame(_series_frame([None] * 5), "series", "fred", "UNRATE")
    assert violations and violations[0].severity == "warn"
    assert "NoNewData" in violations[0].detail


def test_a_genuine_hole_in_a_series_is_not_a_defect():
    # Unlike OHLCV, a scalar series legitimately has gaps — FRED keeps the row and
    # nulls the value on purpose, so the gap stays visible.
    assert verify_frame(_series_frame([1.0, 2.0, None, 4.0, 5.0]), "series") == []


def test_a_constant_series_is_not_a_defect():
    # A policy rate held flat for months is the most important kind of macro series,
    # not a broken one. The variance check that catches a constant *price* would
    # fire on every one of them.
    assert verify_frame(_series_frame([3.5] * 5), "series") == []


# --- series self-consistency: the revision blind spot ------------------------------
#
# `update_series` is watermark-advanced, so a value it already holds is never asked
# for again and a revision to it is invisible to the nightly *by design*. Measured
# against live FRED when this was written, the lake held six stale series.


def _scalar(values, columns=("value",), start="2020-01-01"):
    idx = pd.DatetimeIndex(
        pd.date_range(start, periods=len(values), freq="D", tz="UTC"), name="date"
    )
    data = {c: list(values) for c in columns}
    return pd.DataFrame(data, index=idx)


_OBS = [3.5, 3.6, 3.4, 3.9, 4.1, 4.0, 3.8, 3.7]


def _past(frame):
    """Shift a frame so every row is settled (dated before today)."""
    end = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=2)
    idx = pd.date_range(end=end, periods=len(frame), freq="D", tz="UTC", name="date")
    return frame.set_axis(idx)


def test_a_series_that_reproduces_itself_passes():
    stored = _past(_scalar(_OBS))
    assert series_self_consistency(stored, "UNRATE", "fred", loader=lambda *a: stored) == []


def test_a_revised_observation_is_reported_with_its_remediation():
    # Normal for macro data, which is why it is a warn — but the stored copy is now
    # an old vintage, and nothing else in the platform records which vintage it is.
    stored = _past(_scalar(_OBS))
    revised = stored.copy()
    revised.iloc[2, 0] = 99.0
    violations = series_self_consistency(stored, "PAYEMS", "fred", loader=lambda *a: revised)
    assert violations and violations[0].severity == "warn"
    assert "old vintage" in violations[0].detail
    assert "qde.backfill" in violations[0].detail, "a finding should name its fix"


def test_a_revision_in_one_metric_of_many_is_caught():
    # CFTC COT carries eleven trader-category columns. Checking one while trusting
    # ten is a spot check reporting as a guarantee.
    columns = ("dealer_long", "dealer_short", "lev_long")
    stored = _past(_scalar(_OBS, columns=columns))
    revised = stored.copy()
    revised.iloc[3, revised.columns.get_loc("lev_long")] = 12345.0
    violations = series_self_consistency(stored, "VIX", "cftc", loader=lambda *a: revised)
    assert violations and "lev_long" in violations[0].detail


def test_a_metric_that_disappears_is_an_error():
    stored = _past(_scalar(_OBS, columns=("dealer_long", "dealer_short")))
    shrunk = stored[["dealer_long"]]
    violations = series_self_consistency(stored, "VIX", "cftc", loader=lambda *a: shrunk)
    assert violations and violations[0].severity == "error"
    assert "dealer_short" in violations[0].detail


def test_zero_and_negative_values_do_not_break_the_comparison():
    # T10Y2Y inverts and real rates go below zero. A purely relative comparison is
    # undefined at zero and explodes near it, so the tolerance carries an absolute
    # floor as well — this frame must read as clean, not as infinitely revised.
    stored = _past(_scalar([0.0, -0.5, 0.25, 0.0, -1.2, 0.75, 0.0, -0.1]))
    assert series_self_consistency(stored, "T10Y2Y", "fred", loader=lambda *a: stored) == []


def test_a_genuine_change_at_zero_is_still_caught():
    stored = _past(_scalar([0.0, -0.5, 0.25, 0.0, -1.2, 0.75, 0.0, -0.1]))
    revised = stored.copy()
    revised.iloc[0, 0] = 0.4
    assert series_self_consistency(stored, "T10Y2Y", "fred", loader=lambda *a: revised)


def test_dropped_series_history_is_an_error():
    stored = _past(_scalar(_OBS))
    assert any(
        v.severity == "error"
        for v in series_self_consistency(
            stored, "UNRATE", "fred", loader=lambda *a: stored.drop(stored.index[3])
        )
    )


def test_newer_observations_are_not_a_divergence():
    # The normal case: the source has moved on since the watermark. Rows *after* the
    # stored window are new data, not a back-fill.
    stored = _past(_scalar(_OBS))
    extra = pd.DataFrame(
        {"value": [4.4]},
        index=pd.DatetimeIndex([stored.index.max() + pd.Timedelta(days=1)], name="date"),
    )
    assert series_self_consistency(
        stored, "UNRATE", "fred", loader=lambda *a: pd.concat([stored, extra])
    ) == []


def test_an_unreachable_source_is_unverified_not_clean():
    stored = _past(_scalar(_OBS))

    def boom(*a):
        raise TimeoutError("fred down")

    violations = series_self_consistency(stored, "UNRATE", "fred", loader=boom)
    assert violations and "unverified" in violations[0].detail


def test_an_empty_refetch_is_unverified_not_clean():
    stored = _past(_scalar(_OBS))
    violations = series_self_consistency(
        stored, "UNRATE", "fred", loader=lambda *a: stored.iloc[0:0]
    )
    assert violations and violations[0].severity == "warn"


def test_a_repeated_date_in_the_refetch_is_reported_not_a_crash():
    # `.loc[shared]` returns more rows than `shared` has, so a positional comparison
    # raises on the shape mismatch and an aligned one quietly compares a cartesian
    # product. Neither is an answer, and the duplicate is itself the defect.
    stored = _past(_scalar(_OBS))
    doubled = pd.concat([stored, stored.iloc[[2]]])
    violations = series_self_consistency(stored, "UNRATE", "fred", loader=lambda *a: doubled)
    assert violations and violations[0].severity == "error"
    assert "more than once" in violations[0].detail


def test_bars_reports_a_repeated_date_the_same_way():
    stored = _yesterdays(_TRUTH)
    doubled = pd.concat([stored, stored.iloc[[1]]])
    assert any(
        v.severity == "error" and "more than once" in v.detail
        for v in self_consistency(stored, "SPY", "tiingo", loader=lambda *a, **k: doubled)
    )


def test_a_timezone_naive_refetch_is_a_finding_not_a_typeerror():
    stored = _past(_scalar(_OBS))
    naive = stored.copy()
    naive.index = naive.index.tz_localize(None)
    violations = series_self_consistency(stored, "UNRATE", "fred", loader=lambda *a: naive)
    assert violations and violations[0].severity == "error"
    assert "timezone-naive" in violations[0].detail


def test_nothing_comparable_does_not_read_as_verified():
    # Every shared date null on one side: the comparison ran and established nothing.
    import numpy as np

    stored = _past(_scalar(_OBS))
    stored["value"] = np.nan
    violations = series_self_consistency(stored, "UNRATE", "fred", loader=lambda *a: stored)
    assert violations and "unverified" in violations[0].detail


def test_a_metric_the_source_added_is_surfaced():
    stored = _past(_scalar(_OBS))
    wider = stored.copy()
    wider["new_metric"] = 1.0
    violations = series_self_consistency(stored, "VIX", "cftc", loader=lambda *a: wider)
    assert violations and "new_metric" in violations[0].detail


# --- a declaration is not evidence -------------------------------------------------


def test_a_peer_that_declares_the_symbol_but_holds_no_data_is_not_a_peer(tmp_path, monkeypatch):
    # The registry states *intent* — the set someone meant to backfill. Treating that
    # as evidence reported BTCUSDT as "corroborated" by yfinance, which carries no
    # BTCUSDT row at all, and published the claim in catalogue.json. The tier built to
    # stop the platform overclaiming was overclaiming itself.
    from qde.verify import verification_status

    monkeypatch.setattr(
        "qde.registry.declared_series",
        lambda group=None: [("binance", "BTCUSDT", "1d"), ("ghost", "BTCUSDT", "1d")],
    )
    monkeypatch.setattr(
        "qde.storage.list_bars_series",
        lambda base_dir: pd.DataFrame(
            [{"source": "binance", "symbol": "BTCUSDT", "interval": "1d"}]
        ),
    )

    status = verification_status("BTCUSDT", "binance", base_dir=str(tmp_path))
    assert status["peers"] == [], "a declaration with no rows behind it is not evidence"
    assert status["level"] != "corroborated"


def test_an_unreadable_lake_downgrades_rather_than_restores_optimism(tmp_path, monkeypatch):
    # If the lake cannot be read we do not know which peers are real. Falling back to
    # the registry would quietly reinstate exactly the overclaim above.
    from qde.verify import verification_status

    monkeypatch.setattr(
        "qde.registry.declared_series",
        lambda group=None: [("binance", "BTCUSDT", "1d"), ("okx", "BTCUSDT", "1d")],
    )

    def boom(base_dir):
        raise OSError("lake gone")

    monkeypatch.setattr("qde.storage.list_bars_series", boom)
    assert verification_status("BTCUSDT", "binance", base_dir=str(tmp_path))["peers"] == []


# --- the series group has TWO legal shapes -----------------------------------------
#
# Every fixture above used a single `value` column, so requiring that name passed the
# whole suite while rejecting binancefut and CFTC at intake, at error severity, in
# production, for two consecutive nights.


def test_a_multi_metric_series_frame_is_accepted():
    # `upsert_series_frame` writes one file per column under a metric= partition; this
    # is the shape binancefut (funding_rate, mark_price) and CFTC actually send.
    frame = _series_frame([1.0, 2.0, 3.0])
    frame = frame.rename(columns={"value": "funding_rate"})
    frame["mark_price"] = [10.0, 11.0, 12.0]
    assert verify_frame(frame, "series", "binancefut", "BTCUSDT") == []


def test_a_single_value_series_frame_is_still_accepted():
    assert verify_frame(_series_frame([1.0, 2.0, 3.0]), "series", "fred", "UNRATE") == []


def test_every_metric_is_parse_checked_not_just_one_named_value():
    # Hardcoding "value" skipped the parse check entirely for the sources carrying the
    # most columns — the opposite of where it is most needed.
    frame = _series_frame([1.0, 2.0, 3.0]).rename(columns={"value": "funding_rate"})
    frame["open_interest"] = ["1,234", "2,345", "3,456"]
    violations = verify_frame(frame, "series", "binancefut", "BTCUSDT")
    assert violations and violations[0].check == "numeric"
    assert "open_interest" in violations[0].detail


def test_a_series_frame_with_no_columns_at_all_is_refused():
    idx = pd.DatetimeIndex(pd.date_range("2024-01-01", periods=3, freq="D", tz="UTC"))
    assert verify_frame(pd.DataFrame(index=idx), "series", "s", "X")


@pytest.mark.parametrize(
    "columns",
    [("value",), ("funding_rate", "mark_price"), ("dealer_long", "dealer_short", "lev_long")],
)
def test_the_contract_matches_what_storage_accepts(columns):
    # The invariant that would have caught this: anything upsert_series_frame can write
    # must pass verification, or intake rejects data the lake has always stored.
    frame = _series_frame([1.0, 2.0, 3.0], columns=columns)
    assert verify_frame(frame, "series", "s", "X") == []
