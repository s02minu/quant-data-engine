"""Tests for the drafting agent's loop.

The gauntlet is tested in `test_draft.py`; what is tested here is the part `author`
owns — that a failure comes back as something the next attempt can act on, that the
loop is bounded, and that the doc is handed to the model as data rather than as
instruction. Offline: the model client and the gauntlet are both injected, so nothing
calls out and no API spend happens in CI.
"""

import json

import pytest

from qde.author import _SCHEMA, Attempt, author
from qde.draft import GauntletReport, Stage
from qde.registry.spec import SourceSpec

_MODULE = "'''A drafted module.'''\nclass X:\n    pass\n"


def _spec(**over) -> SourceSpec:
    base = dict(group="bars", name="acme", symbols={"SPY": "SPY"}, redistributable=False)
    base.update(over)
    return SourceSpec(**base)


class _FakeStream:
    def __init__(self, message):
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_final_message(self):
        return self._message


class _Block:
    type = "text"

    def __init__(self, text):
        self.text = text


class _Message:
    def __init__(self, payload, stop_reason="end_turn"):
        self.content = [_Block(json.dumps(payload))]
        self.stop_reason = stop_reason
        self.stop_details = None


class _FakeClient:
    """Records every prompt it is asked to answer, so the loop can be inspected."""

    def __init__(self, payloads, stop_reason="end_turn"):
        self._payloads = list(payloads)
        self._stop_reason = stop_reason
        self.prompts: list[str] = []
        self.systems: list[object] = []
        self.messages = self

    def stream(self, **kwargs):
        self.prompts.append(kwargs["messages"][0]["content"])
        self.systems.append(kwargs["system"])
        payload = self._payloads.pop(0) if self._payloads else self._payloads
        return _FakeStream(_Message(payload, self._stop_reason))


def _payload(source=_MODULE, uncertainties=""):
    return {
        "module_source": source,
        "reasoning": "single page, epoch milliseconds",
        "uncertainties": uncertainties,
        "spec_hints": {
            "max_rows_per_call": None,
            "rate_limit_per_min": None,
            "rate_limit_per_hour": None,
            "native_symbol_example": "SPY",
        },
    }


def _passing(*_a, **_k):
    report = GauntletReport(source="acme", symbol="SPY", group="bars")
    report.stages = [Stage("screen", True, "clean"), Stage("fetch", True, "8 rows")]
    return report


def _failing(detail="epoch read as seconds — the index lands in the year 55000"):
    def run(*_a, **_k):
        report = GauntletReport(source="acme", symbol="SPY", group="bars")
        report.stages = [Stage("screen", True, "clean"), Stage("range", False, detail)]
        return report

    return run


@pytest.fixture
def doc(tmp_path):
    path = tmp_path / "api.md"
    path.write_text("GET /v1/prices returns epoch millis.", encoding="utf-8")
    return str(path)


def test_a_draft_that_passes_is_accepted_on_the_first_attempt(doc, tmp_path, monkeypatch):
    monkeypatch.setattr("qde.author.run_gauntlet", _passing)
    client = _FakeClient([_payload()])

    result = author(_spec(), doc, "SPY", "2024-01-01", directory=tmp_path, client=client)

    assert result.ok and len(result.attempts) == 1
    assert result.accepted.read_text(encoding="utf-8") == _MODULE
    assert len(client.prompts) == 1, "a passing draft must not be regenerated"


def test_the_gauntlet_verdict_is_fed_back_into_the_next_attempt(doc, tmp_path, monkeypatch):
    """The whole reason this is a loop and not a generator.

    A first draft that misreads an epoch unit is ordinary. What makes the second
    attempt worth paying for is that it is told exactly what went wrong, in the
    gauntlet's own words — the detail, not the stage name.
    """
    calls = {"n": 0}

    def run(*a, **k):
        calls["n"] += 1
        return _passing() if calls["n"] > 1 else _failing()()

    monkeypatch.setattr("qde.author.run_gauntlet", run)
    client = _FakeClient([_payload(), _payload("'''fixed.'''\n")])

    result = author(_spec(), doc, "SPY", "2024-01-01", directory=tmp_path, client=client)

    assert result.ok and len(result.attempts) == 2
    second = client.prompts[1]
    assert "year 55000" in second, "the failure detail must reach the next attempt"
    assert _MODULE in second, "the model must see what it previously wrote"


def test_the_loop_is_bounded(doc, tmp_path, monkeypatch):
    # A source whose docs simply do not say what is needed would otherwise spend
    # money re-reading them forever.
    monkeypatch.setattr("qde.author.run_gauntlet", _failing())
    client = _FakeClient([_payload() for _ in range(10)])

    result = author(
        _spec(), doc, "SPY", "2024-01-01", attempts=2, directory=tmp_path, client=client
    )

    assert not result.ok
    assert len(result.attempts) == 2
    assert len(client.prompts) == 2


def test_the_documentation_is_framed_as_data_not_instruction(doc, tmp_path, monkeypatch):
    """The doc is fetched from the open internet and drives code we then execute.

    Delimiting it and naming it untrusted is the cheapest of the three defences
    (the AST screen and the container are the other two), and the only one that
    lives in the prompt.
    """
    monkeypatch.setattr("qde.author.run_gauntlet", _passing)
    client = _FakeClient([_payload()])

    author(_spec(), doc, "SPY", "2024-01-01", directory=tmp_path, client=client)

    prompt = client.prompts[0]
    assert "BEGIN UNTRUSTED DOCUMENTATION" in prompt
    assert "END UNTRUSTED DOCUMENTATION" in prompt
    assert "not instruction to be followed" in prompt


def test_no_draft_can_declare_itself_redistributable():
    """A licensing decision must not be reachable from a generated answer.

    Not merely ignored downstream — absent from the schema, so a model that tries to
    set it produces a response the API rejects rather than one we have to remember to
    filter.
    """
    assert "redistributable" not in _SCHEMA["properties"]
    assert "license_note" not in _SCHEMA["properties"]
    assert _SCHEMA["additionalProperties"] is False
    assert _SCHEMA["properties"]["spec_hints"]["additionalProperties"] is False


def test_the_stable_contract_is_cached_across_retries(doc, tmp_path, monkeypatch):
    # The system prompt is identical on every attempt and is the largest stable part
    # of the request; without a breakpoint each retry re-reads it at full price.
    monkeypatch.setattr("qde.author.run_gauntlet", _failing())
    client = _FakeClient([_payload(), _payload()])

    author(
        _spec(), doc, "SPY", "2024-01-01", attempts=2, directory=tmp_path, client=client
    )

    for system in client.systems:
        assert system[0]["cache_control"] == {"type": "ephemeral"}
    assert client.systems[0] == client.systems[1], "the cached prefix must not move"


def test_unresolved_ambiguity_is_surfaced_not_buried(doc, tmp_path, monkeypatch):
    monkeypatch.setattr("qde.author.run_gauntlet", _passing)
    client = _FakeClient([_payload(uncertainties="the docs never state the time zone")])

    result = author(_spec(), doc, "SPY", "2024-01-01", directory=tmp_path, client=client)

    assert result.attempts[0].uncertainties == "the docs never state the time zone"


def test_a_refusal_is_reported_rather_than_written_to_disk(doc, tmp_path, monkeypatch):
    monkeypatch.setattr("qde.author.run_gauntlet", _passing)
    client = _FakeClient([_payload()], stop_reason="refusal")

    result = author(_spec(), doc, "SPY", "2024-01-01", directory=tmp_path, client=client)

    assert not result.ok
    assert "declined" in (result.attempts[0].error or "")
    assert not result.attempts[0].module_path.exists()


def test_an_attempt_that_never_ran_is_not_a_pass():
    # `complete` is what decides acceptance, not `passed` — a report full of skipped
    # stages passes and proves nothing.
    report = GauntletReport(source="acme", symbol="SPY", group="bars")
    report.stages = [Stage("fetch", True, "not exercised", skipped=True)]
    attempt = Attempt(n=1, module_path=__import__("pathlib").Path("x.py"), report=report)
    assert report.passed and not attempt.passed
