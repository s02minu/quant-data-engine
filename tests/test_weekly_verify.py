"""Tests for the weekly deep-verification pass.

Offline by construction: both loaders are injected, so nothing here touches the
network or a real lake. The nightly's tests cover what can be read from storage;
these cover the two checks that need a second look at the source itself.
"""

import numpy as np
import pandas as pd

from qde import weekly_verify


def _frame(values, end_offset=2):
    idx = pd.date_range(
        end=pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=end_offset),
        periods=len(values),
        freq="D",
        tz="UTC",
        name="date",
    )
    return pd.DataFrame(
        {"open": values, "high": values, "low": values, "close": values,
         "volume": np.ones(len(values))},
        index=idx,
    )


def _lake(monkeypatch, frames):
    """Stand in for the lake's series listing and its local reader."""
    listing = pd.DataFrame(
        [{"symbol": s, "source": src, "interval": "1d"} for (src, s) in frames]
    )
    monkeypatch.setattr(weekly_verify, "list_bars_series", lambda base_dir: listing)
    monkeypatch.setattr(weekly_verify, "list_series",
                        lambda base_dir: pd.DataFrame(columns=["source", "series_id"]))
    monkeypatch.setattr(weekly_verify, "list_series",
                        lambda base_dir: pd.DataFrame(columns=["source", "series_id"]))

    def local(symbol, source=None, interval="1d", base_dir="data"):
        return frames[(source, symbol)]

    return local


def _walk(n, seed):
    rng = np.random.default_rng(seed)
    return 100.0 * np.exp(np.cumsum(rng.normal(0, 0.02, n)))


def test_a_source_that_reproduces_itself_is_counted_as_checked(monkeypatch):
    frames = {("s", "AAA"): _frame(_walk(700, 1))}
    local = _lake(monkeypatch, frames)
    result = weekly_verify.run("data", loader=lambda *a, **k: frames[("s", "AAA")],
                               local_loader=local)
    assert result["checked"] == 1
    assert result["failed"] == 0


def test_a_revised_history_is_reported(monkeypatch):
    stored = _frame(_walk(700, 2))
    revised = stored.copy()
    revised.iloc[5, revised.columns.get_loc("close")] *= 1.5
    local = _lake(monkeypatch, {("s", "AAA"): stored})

    result = weekly_verify.run("data", loader=lambda *a, **k: revised, local_loader=local)
    assert any(v.check == "self_consistency" for v in result["violations"])


def test_one_broken_series_does_not_abort_the_rest(monkeypatch):
    # The nightly's hard-won property: a single failure is recorded and skipped, so
    # the pass still covers everything else. A run that aborts halfway reports far
    # fewer violations than a clean one, which reads as an improvement.
    good = _frame(_walk(700, 3))
    listing = pd.DataFrame(
        [{"symbol": "BAD", "source": "s", "interval": "1d"},
         {"symbol": "AAA", "source": "s", "interval": "1d"}]
    )
    monkeypatch.setattr(weekly_verify, "list_bars_series", lambda base_dir: listing)
    monkeypatch.setattr(weekly_verify, "list_series",
                        lambda base_dir: pd.DataFrame(columns=["source", "series_id"]))

    def local(symbol, source=None, interval="1d", base_dir="data"):
        if symbol == "BAD":
            raise FileNotFoundError("BAD")
        return good

    result = weekly_verify.run("data", loader=lambda *a, **k: good, local_loader=local)
    assert result["checked"] == 1, "the healthy series must still be checked"
    assert result["failed"] == 1
    assert result["failures"][0]["error"] == "FileNotFoundError"


def test_a_series_that_could_not_be_checked_is_never_counted_as_passing(monkeypatch):
    listing = pd.DataFrame([{"symbol": "BAD", "source": "s", "interval": "1d"}])
    monkeypatch.setattr(weekly_verify, "list_bars_series", lambda base_dir: listing)
    monkeypatch.setattr(weekly_verify, "list_series",
                        lambda base_dir: pd.DataFrame(columns=["source", "series_id"]))

    def local(symbol, source=None, interval="1d", base_dir="data"):
        raise OSError("disk gone")

    result = weekly_verify.run("data", local_loader=local)
    assert result["checked"] == 0 and result["failed"] == 1


def test_intraday_series_are_skipped(monkeypatch):
    # Re-fetching a minute series to answer a daily-granularity question would cost
    # hundreds of thousands of rows for nothing.
    listing = pd.DataFrame([{"symbol": "AAA", "source": "s", "interval": "1m"}])
    monkeypatch.setattr(weekly_verify, "list_bars_series", lambda base_dir: listing)
    monkeypatch.setattr(weekly_verify, "list_series",
                        lambda base_dir: pd.DataFrame(columns=["source", "series_id"]))

    def local(symbol, source=None, interval="1d", base_dir="data"):
        raise AssertionError("must not be read")

    assert weekly_verify.run("data", local_loader=local)["checked"] == 0


def test_a_standing_no_proxy_finding_is_recorded_but_never_alerted(monkeypatch):
    # The whole point of splitting the check names: everything reaches dq_violations,
    # only actionable findings reach Discord.
    from qde.checks import Violation
    from qde.verify import PROXY, PROXY_UNAVAILABLE

    sent = []
    monkeypatch.setattr("qde.alert.send_discord", lambda msg: sent.append(msg))
    monkeypatch.setattr(weekly_verify, "list_bars_series",
                        lambda base_dir: pd.DataFrame(columns=["symbol", "source", "interval"]))
    recorded = []
    monkeypatch.setattr(weekly_verify, "record_run",
                        lambda v, b, cadence=None: recorded.extend(v))

    def only(check):
        return [Violation("bars", "s", "GLD", None, check, "warn", "d")]

    monkeypatch.setattr(weekly_verify, "run", lambda base_dir: {
        "checked": 1, "failed": 0, "failures": [], "violations": only(PROXY_UNAVAILABLE)})
    weekly_verify.main()
    assert recorded, "it must still be recorded"
    assert not sent, "a standing property must not alert"

    recorded.clear()
    monkeypatch.setattr(weekly_verify, "run", lambda base_dir: {
        "checked": 1, "failed": 0, "failures": [], "violations": only(PROXY)})
    weekly_verify.main()
    assert sent, "an actual break must alert"
    assert "weekly" in sent[0], "and must not claim to be the nightly"


def test_scalar_series_are_checked_for_revisions(monkeypatch):
    # The blind spot this closes: update_series is watermark-advanced, so a value it
    # already holds is never re-requested and a revision to it is invisible by design.
    listing = pd.DataFrame([{"source": "fred", "series_id": "PAYEMS"}])
    monkeypatch.setattr(weekly_verify, "list_bars_series",
                        lambda base_dir: pd.DataFrame(columns=["symbol", "source", "interval"]))
    monkeypatch.setattr(weekly_verify, "list_series", lambda base_dir: listing)

    idx = pd.date_range(
        end=pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=2),
        periods=6, freq="D", tz="UTC", name="date",
    )
    stored = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}, index=idx)
    revised = stored.copy()
    revised.iloc[1, 0] = 99.0

    result = weekly_verify.run(
        "data",
        series_local_loader=lambda sid, src, base_dir="data": stored,
        series_loader=lambda sid, src, start: revised,
    )
    assert result["checked"] == 1
    assert any(v.group == "series" and v.check == "self_consistency"
               for v in result["violations"])


def test_an_unreadable_scalar_series_is_a_failure_not_a_pass(monkeypatch):
    listing = pd.DataFrame([{"source": "fred", "series_id": "GONE"}])
    monkeypatch.setattr(weekly_verify, "list_bars_series",
                        lambda base_dir: pd.DataFrame(columns=["symbol", "source", "interval"]))
    monkeypatch.setattr(weekly_verify, "list_series", lambda base_dir: listing)

    def missing(sid, src, base_dir="data"):
        raise FileNotFoundError(sid)

    result = weekly_verify.run("data", series_local_loader=missing)
    assert result["checked"] == 0 and result["failed"] == 1
