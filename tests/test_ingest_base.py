"""Tests for the shared ingestion machinery and the HTTP boundary.

Both guarded here fail the same way when they go wrong: the nightly does not
crash, it simply never finishes. Nothing raises, nothing alerts, and the process
looks healthy — so these exist to convert a hang into an error.
"""

import pandas as pd
import pytest
import requests

from qde.ingest.base import MAX_PAGES, BaseIngestor, RawPage
from qde.loaders.exceptions import NoNewData
from qde.loaders.http import get_with_requests
from qde.registry import SOURCES


class _Ingestor(BaseIngestor):
    """Drives the shared loop with a scripted sequence of pages."""

    def __init__(self, spec, pages):
        super().__init__(spec)
        self._pages = list(pages)
        self.calls = 0

    def first_cursor(self, symbol, start, end, interval):
        return 0

    def fetch_page(self, symbol, cursor, start, end, interval):
        self.calls += 1
        return self._pages[min(self.calls - 1, len(self._pages) - 1)]

    def normalize(self, rows):
        return pd.DataFrame({"close": rows})


@pytest.fixture
def spec():
    return SOURCES["binance"]


def test_a_normal_walk_accumulates_every_page(spec):
    ing = _Ingestor(spec, [RawPage([1, 2], 1), RawPage([3], 2), RawPage([4], None)])
    assert list(ing.load_native("BTCUSDT", "2024-01-01")["close"]) == [1, 2, 3, 4]


def test_an_empty_walk_is_not_new_data(spec):
    # The benign "already up to date" case must stay distinguishable from a failure.
    ing = _Ingestor(spec, [RawPage([], None)])
    with pytest.raises(NoNewData):
        ing.load_native("BTCUSDT", "2024-01-01")


def test_a_cursor_that_never_moves_raises_instead_of_looping_forever(spec):
    # A source handing back a cursor it already issued would otherwise be walked
    # forever, accumulating rows in memory against a remote API.
    ing = _Ingestor(spec, [RawPage([1], 7), RawPage([1], 7)])
    with pytest.raises(ValueError, match="pagination stalled"):
        ing.load_native("BTCUSDT", "2024-01-01")
    assert ing.calls < 5, "should stop as soon as the cursor repeats"


def test_an_unhashable_cursor_is_still_guarded(spec):
    # Some sources page with a dict or list cursor; the guard must not fall over on
    # them, or the protection quietly does not apply to exactly those sources.
    ing = _Ingestor(spec, [RawPage([1], {"page": 1}), RawPage([1], {"page": 1})])
    with pytest.raises(ValueError, match="pagination stalled"):
        ing.load_native("BTCUSDT", "2024-01-01")


def test_a_walk_that_advances_forever_still_terminates(spec):
    # A cursor that always moves defeats the repeated-cursor check, so the page
    # ceiling is the backstop.
    class Forever(_Ingestor):
        def fetch_page(self, symbol, cursor, start, end, interval):
            self.calls += 1
            return RawPage([1], self.calls)

    ing = Forever(spec, [])
    with pytest.raises(ValueError, match=f"exceeded {MAX_PAGES} pages"):
        ing.load_native("BTCUSDT", "2024-01-01")


# --- the HTTP boundary ----------------------------------------------------------


class _Resp:
    def __init__(self, status_code):
        self.status_code = status_code


def test_every_request_carries_a_timeout(monkeypatch):
    # Without one, a connection that opens and then goes silent blocks forever and
    # the nightly never finishes — alive, quiet, and producing nothing.
    seen = {}

    def fake_get(url, params=None, **kwargs):
        seen.update(kwargs)
        return _Resp(200)

    monkeypatch.setattr(requests, "get", fake_get)
    get_with_requests("http://x", {})

    assert "timeout" in seen and seen["timeout"] is not None


def test_a_transport_failure_is_retried_not_fatal(monkeypatch):
    # A dropped connection or slow DNS answer is exactly what the backoff is for;
    # failing the source outright reports a gap that never really existed.
    calls = {"n": 0}

    def fake_get(url, params=None, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise requests.exceptions.ConnectionError("reset")
        return _Resp(200)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr("qde.loaders.http.time.sleep", lambda _s: None)

    assert get_with_requests("http://x", {}).status_code == 200
    assert calls["n"] == 3


def test_persistent_transport_failure_reports_the_cause(monkeypatch):
    def fake_get(url, params=None, **kwargs):
        raise requests.exceptions.ReadTimeout("too slow")

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr("qde.loaders.http.time.sleep", lambda _s: None)

    with pytest.raises(ValueError, match="too slow"):
        get_with_requests("http://x", {}, max_retries=2)


def test_a_client_error_is_not_retried(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, params=None, **kwargs):
        calls["n"] += 1
        return _Resp(404)

    monkeypatch.setattr(requests, "get", fake_get)
    with pytest.raises(ValueError, match="404"):
        get_with_requests("http://x", {})
    assert calls["n"] == 1, "retrying a bad request just burns the rate limit"


# --- the declared rate limit is a control, not a comment --------------------------


def test_a_declared_rate_limit_paces_calls(monkeypatch):
    """`rate_limit_per_min` sat in the registry unenforced for months.

    It became load-bearing with Tiingo, whose free tier caps requests per hour: one
    nightly over 27 symbols fits, a manual re-run in the same hour does not. Without
    pacing the only defence is retry/backoff, which turns a predictable wait into
    four failed attempts and an exception.
    """
    import qde.ingest.base as base

    slept: list[float] = []
    monkeypatch.setattr(base.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(base.time, "monotonic", lambda: 1000.0)  # no time passes
    base._LAST_CALL.clear()

    base._throttle("slowsource", 60)  # first call: nothing to wait for
    base._throttle("slowsource", 60)  # second: must wait the full interval

    assert slept and abs(slept[0] - 1.0) < 1e-6, "60/min means one second apart"


def test_a_source_without_a_limit_is_never_delayed(monkeypatch):
    import qde.ingest.base as base

    slept: list[float] = []
    monkeypatch.setattr(base.time, "sleep", lambda s: slept.append(s))
    base._LAST_CALL.clear()

    for _ in range(5):
        base._throttle("fastsource", None)
    assert slept == [], "an unlimited source must not pay for the mechanism"


def test_pacing_is_shared_across_ingestor_instances(monkeypatch):
    # get_ingestor builds a fresh object per symbol, so per-instance state would pace
    # nothing across a 27-symbol run — which is exactly the case that matters.
    import qde.ingest.base as base

    slept: list[float] = []
    monkeypatch.setattr(base.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(base.time, "monotonic", lambda: 500.0)
    base._LAST_CALL.clear()

    base._throttle("shared", 30)
    base._throttle("shared", 30)  # a different "instance" would still be paced
    assert slept, "the limit must hold across the whole run, not per object"
