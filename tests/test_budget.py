"""Tests for the cross-process hourly request budget.

Each case is a way the *previous* limiter failed silently. `rate_limit_per_min` was
enforced by a dict in one process, so a second container, a re-run, or a retry storm
all spent an allowance the source was still counting. Nothing here touches a network.
"""

import os

import pytest

import qde.loaders.http as http_mod
from qde.loaders import budget
from qde.loaders.budget import RateBudgetExhausted

from .conftest import FakeResponse

_HOUR = 3600.0
_T0 = 1_800_000_000.0  # an arbitrary instant, mid-hour


@pytest.fixture
def metered(monkeypatch):
    """A source declaring a small hourly cap, so the limit is reachable in a test."""
    monkeypatch.setattr(budget, "_hourly_limit", lambda source: 3 if source == "capped" else None)
    return "capped"


def test_a_source_declaring_no_hourly_limit_is_never_metered(metered):
    # Every exchange that pages freely goes down this path; it must cost nothing and
    # must not create a ledger file.
    for _ in range(50):
        budget.consume("uncapped")
    assert budget.spent_this_hour("uncapped") == 0


def test_spend_below_the_limit_passes(metered):
    for _ in range(3):
        budget.consume(metered, now=_T0)
    assert budget.spent_this_hour(metered, now=_T0) == 3


def test_the_request_past_the_limit_is_refused(metered):
    for _ in range(3):
        budget.consume(metered, now=_T0)
    with pytest.raises(RateBudgetExhausted) as excinfo:
        budget.consume(metered, now=_T0)
    # The message has to say when to come back; a bare "limit exceeded" leaves the
    # operator guessing at exactly the moment they are trying to unblock a nightly.
    assert "resets at" in str(excinfo.value)


def test_the_allowance_refills_at_the_top_of_the_hour(metered):
    # Tiingo documents "Hourly Requests - Reset every hour", so this is a fixed
    # window. A sliding one would still be refusing calls the API would have served.
    for _ in range(3):
        budget.consume(metered, now=_T0)
    budget.consume(metered, now=_T0 + _HOUR)
    assert budget.spent_this_hour(metered, now=_T0 + _HOUR) == 1


def test_the_spend_outlives_the_process_that_made_it(metered):
    """The whole point, and the one claim a same-process test cannot make.

    A genuinely separate interpreter -- not `importlib.reload`, which rebuilds the
    exception class and quietly breaks every `pytest.raises` after it. This is the
    nightly-then-manual-rerun sequence that the old per-process dict could not see:
    if the count lived in memory the subprocess reports 0 and the second container
    spends the hour all over again.
    """
    import subprocess
    import sys

    for _ in range(3):
        budget.consume(metered, now=_T0)

    probe = subprocess.run(
        [sys.executable, "-c",
         "from qde.loaders import budget;"
         f"print(budget.spent_this_hour('capped', now={_T0}))"],
        capture_output=True, text=True, env={**os.environ},
    )
    assert probe.returncode == 0, probe.stderr
    assert probe.stdout.strip() == "3", f"a second process saw {probe.stdout!r}, not the spend"


def test_pruning_never_drops_a_live_entry(metered, monkeypatch):
    monkeypatch.setattr(budget, "_hourly_limit", lambda source: 100)

    # The ledger is pruned opportunistically. Losing a current-window line would
    # silently hand back allowance that was already spent.
    for _ in range(9):
        budget.consume(metered, now=_T0 - 5 * _HOUR)
    for _ in range(2):
        budget.consume(metered, now=_T0)
    assert budget.spent_this_hour(metered, now=_T0) == 2


# --- the HTTP boundary ------------------------------------------------------------


def test_every_retry_is_charged_not_just_the_first_attempt(metered, monkeypatch):
    """A 429 storm used to cost four unmetered requests.

    Retrying is the one moment the budget matters most -- a 429 *is* the source
    saying the allowance is gone -- and it was the one moment nothing was counting.
    """
    monkeypatch.setattr(budget, "_hourly_limit", lambda source: 50)
    monkeypatch.setattr(http_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        http_mod.requests, "get", lambda *a, **k: FakeResponse(status_code=429)
    )

    with pytest.raises(ValueError):
        http_mod.get_with_requests("https://example.test/x", params=None, source="capped")

    assert budget.spent_this_hour("capped") == 4


def test_an_exhausted_budget_stops_the_request_before_the_socket_opens(metered, monkeypatch):
    calls = []
    monkeypatch.setattr(http_mod.requests, "get", lambda *a, **k: calls.append(1))

    for _ in range(3):
        budget.consume(metered)
    with pytest.raises(RateBudgetExhausted):
        http_mod.get_with_requests("https://example.test/x", params=None, source=metered)
    assert calls == [], "the budget must refuse before any request is issued"


def test_the_fetch_context_supplies_the_source_without_per_caller_wiring(metered, monkeypatch):
    """Ingestors call `get_with_requests` themselves and never pass a source.

    `BaseIngestor` sets the context once, so a source added later is metered without
    anyone remembering to thread an argument through its `fetch_page`.
    """
    monkeypatch.setattr(http_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        http_mod.requests, "get", lambda *a, **k: FakeResponse(status_code=200, payload=[])
    )

    with budget.fetching(metered):
        http_mod.get_with_requests("https://example.test/x", params=None)
    assert budget.spent_this_hour(metered) == 1

    # and outside the context nothing is attributed
    http_mod.get_with_requests("https://example.test/x", params=None)
    assert budget.spent_this_hour(metered) == 1


def test_tiingo_declares_the_quota_its_provider_actually_enforces():
    """50/hour, from Tiingo's pricing page — the registry must not drift from it.

    This is the row that turns the machinery above into an actual control. If the
    field goes back to None the limiter silently becomes a no-op for the one source
    that needed it.
    """
    from qde.registry import all_specs

    spec = next(s for s in all_specs() if s.name == "tiingo")
    assert spec.rate_limit_per_hour == 50
