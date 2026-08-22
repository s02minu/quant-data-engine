"""Scaffold a new source's ingestor, then prove it against the live API.

Adding a source by hand means writing three methods and a registry row, then
convincing yourself the result is right — and "convincing yourself" is the part that
does not scale and does not survive being done by a generator. A model can draft the
three methods from an API's documentation in seconds. Nothing about that draft tells
you whether it *works*, and the failure mode is not a crash: it is a frame that
arrives complete, correctly typed and entirely plausible while being the wrong data
(see :mod:`qde.verify`).

So this module is deliberately **not a code generator**. Generating the code is the
commodity half and improves on its own; deciding whether the result can be trusted is
the specific half, and it is what a human review was previously for. The gauntlet
below replaces that review with something a generator can *iterate against*: every
stage returns a structured verdict, so a drafting agent reads the failures and fixes
its own output without a person in the loop.

What the gauntlet establishes, in order of what each can rule out:

1. ``contract``     — the module loads and implements the ingestor interface at all.
2. ``fetch``        — a real pull against the real API returns rows.
3. ``frame``        — the result satisfies its group's contract (:func:`qde.verify.verify_frame`).
4. ``determinism``  — asking the same question twice gives the same answer. Catches
                      pagination that depends on wall-clock time, an unstable sort, a
                      cursor that skips, and a cache serving different pages.
5. ``range``        — the source honours ``start``/``end`` rather than ignoring them,
                      which is the difference between a window and a coincidence.
6. ``pagination``   — a window wider than one page really walks, instead of silently
                      returning the first page and stopping.
7. ``cross_source`` — where another source already carries the symbol, the numbers
                      agree (:func:`qde.verify.cross_check`).

**Two things this will never decide.** ``redistributable`` is a licensing judgment
with legal consequences, not a technical property that can be measured from a
response — it defaults to False and only a person may change it. And a passing
gauntlet is evidence, not proof: it says the ingestor is right about the window it
was asked for, on the day it was asked.

**THIS IS NOT A SANDBOX.** Proving a draft means *running* it, and importing a Python
module executes its top-level code with this process's filesystem, network and
environment. Quarantine keeps an unproven module out of the *pipeline*; it does
nothing to contain a hostile one. A draft generated from documentation fetched off
the internet is exactly the shape of thing that carries a prompt injection, and in
this repository a draft that ran unchecked could read every ``secrets/*.env`` file
and post them somewhere.

So executing a draft is treated as a decision rather than a default: ``run_gauntlet``
refuses unless the caller states the code is trusted, the environment it runs in is
cut down to the one credential the source actually needs, and an AST screen rejects
the obvious cases first. **None of that is containment** — a determined draft still
runs as you. For anything you did not write yourself, run the gauntlet inside the
project's container, where the blast radius is a throwaway filesystem:

    docker compose run --rm --network none collector \
        python -m qde.draft verify drafts/<file>.py \
        --name <source> --symbol <SYM> --from <DATE> --trust-this-draft
"""

import importlib.util
import inspect
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from qde.ingest.base import BaseIngestor
from qde.registry.spec import SourceSpec

# Where a drafted ingestor lands. Deliberately NOT `src/qde/ingest/`: an unproven
# module inside the package is importable by the nightly, and "not wired up yet" is a
# convention rather than a barrier. Quarantined code has to be moved by hand, which is
# the one human step worth keeping — not reviewing the logic, just consenting to run it.
QUARANTINE = Path("drafts")


@dataclass
class Stage:
    """One gauntlet stage's verdict, shaped to be read by a machine or a person."""

    name: str
    passed: bool
    detail: str
    # A failed blocking stage stops the run: there is no sense asking whether the
    # frame is plausible when the module did not import.
    blocking: bool = False
    # True when the stage did not actually run. `passed` stays True so it does not
    # read as a defect, but the report refuses to call the run complete. A stage
    # that could not run is NOT a stage that passed — conflating them is how this
    # harness reported PASS on an ingestor silently missing a fifth of its history.
    skipped: bool = False


@dataclass
class GauntletReport:
    source: str
    symbol: str
    group: str
    stages: list[Stage] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(s.passed for s in self.stages)

    @property
    def failures(self) -> list[Stage]:
        return [s for s in self.stages if not s.passed]

    @property
    def unexercised(self) -> list[Stage]:
        """Stages that did not run.

        A caller deciding whether to trust a draft has to see these: a green report
        resting on five checks and two shrugs is not the same evidence as one
        resting on seven.
        """
        return [s for s in self.stages if s.skipped]

    @property
    def complete(self) -> bool:
        """Every stage ran and none found a defect."""
        return self.passed and not self.unexercised

    def summary(self) -> str:
        if not self.passed:
            headline = "FAIL"
        elif self.unexercised:
            headline = f"PASS ({len(self.unexercised)} stage(s) not exercised)"
        else:
            headline = "PASS"
        lines = [f"{headline}  {self.source}/{self.symbol} [{self.group}]"]
        for s in self.stages:
            mark = "SKIP" if s.skipped else ("ok  " if s.passed else "FAIL")
            lines.append(f"  {mark}  {s.name:<13} {s.detail}")
        return "\n".join(lines)


# Modules a bar/series ingestor has no business importing. Not a security boundary —
# `__import__("so"+"cket")` walks straight past it — but it turns the careless and the
# obvious into a refusal instead of an execution, and it costs nothing.
_FORBIDDEN_IMPORTS = frozenset(
    {"socket", "subprocess", "shutil", "ctypes", "pickle", "marshal", "smtplib", "ftplib"}
)
# Names whose presence means the draft is doing something an ingestor never needs.
_FORBIDDEN_CALLS = frozenset({"eval", "exec", "compile", "__import__", "breakpoint"})


def screen_source(path: str | Path) -> list[str]:
    """Parse a draft and report constructs an ingestor should never contain.

    Deliberately a *parse*, not an execution: the point is to reject before running.
    An ingestor needs HTTP, pandas and the registry. Reaching for raw sockets,
    subprocesses, or `eval` is either a mistake or something worse, and either way is
    worth stopping at zero cost.

    **This screen is not a security control.** Anything determined defeats it with a
    string concatenation. It exists so the obvious case fails loudly rather than
    silently succeeding — see the module docstring for what actual containment needs.
    """
    import ast

    findings: list[str] = []
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in _FORBIDDEN_IMPORTS:
                    findings.append(f"imports {alias.name!r} (line {node.lineno})")
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".")[0]
            if root in _FORBIDDEN_IMPORTS:
                findings.append(f"imports from {node.module!r} (line {node.lineno})")
        elif isinstance(node, ast.Call):
            name = getattr(node.func, "id", None)
            if name in _FORBIDDEN_CALLS:
                findings.append(f"calls {name}() (line {node.lineno})")
        elif isinstance(node, ast.Attribute) and node.attr in {"system", "popen"}:
            findings.append(f"uses os.{node.attr} (line {node.lineno})")
    return findings


@contextmanager
def _only_this_sources_credentials(source: str):
    """Hide every credential except the one this source legitimately needs.

    A tiingo draft has to hold the Tiingo key to fetch anything; it has no reason to
    see FRED's, or the R2 read keys, or a Discord webhook. Stripping the rest means a
    hostile draft can steal only the secret it was already trusted with.

    Defence in depth, not containment: the `secrets/` files are still on disk and
    still readable. What this removes is the effortless path.
    """
    keep_exact = {f"{source.upper()}_API_KEY", f"{source.upper()}_TOKEN"}
    hidden = {
        key: os.environ[key]
        for key in list(os.environ)
        if any(m in key.upper() for m in ("KEY", "SECRET", "TOKEN", "WEBHOOK", "PASSWORD"))
        and key not in keep_exact
    }
    for key in hidden:
        del os.environ[key]
    try:
        yield
    finally:
        os.environ.update(hidden)


def load_candidate(module_path: str | Path) -> type[BaseIngestor]:
    """Import a drafted module and return its ingestor class.

    Raises:
        ValueError: the module holds no concrete :class:`BaseIngestor` subclass, or
            more than one so the intended entry point is ambiguous.
    """
    path = Path(module_path)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"{path} is not an importable module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    found = [
        obj
        for _name, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, BaseIngestor) and obj is not BaseIngestor
    ]
    if not found:
        raise ValueError(f"{path} defines no BaseIngestor subclass")
    if len(found) > 1:
        raise ValueError(
            f"{path} defines {len(found)} ingestors ({', '.join(c.__name__ for c in found)}); "
            "a draft should expose exactly one"
        )
    return found[0]


def _stage_contract(module_path: str | Path) -> tuple[Stage, type[BaseIngestor] | None]:
    """Does the draft load and implement the interface at all?"""
    try:
        cls = load_candidate(module_path)
    except Exception as exc:
        return Stage("contract", False, f"{type(exc).__name__}: {exc}", blocking=True), None

    missing = [
        name
        for name in ("first_cursor", "fetch_page", "normalize")
        if getattr(cls, name, None) is getattr(BaseIngestor, name, None)
    ]
    if missing:
        return (
            Stage(
                "contract",
                False,
                f"{cls.__name__} does not override: {', '.join(missing)}",
                blocking=True,
            ),
            None,
        )
    return Stage("contract", True, f"{cls.__name__} implements the ingestor interface"), cls


def _stage_fetch(ingestor, symbol: str, start: str, end: str | None, interval: str):
    """Does a real pull against the real API return anything?"""
    try:
        frame = ingestor.load(symbol, start=start, end=end, interval=interval)
    except Exception as exc:
        return Stage("fetch", False, f"{type(exc).__name__}: {exc}", blocking=True), None
    if frame is None or frame.empty:
        return Stage("fetch", False, "returned no rows for the window", blocking=True), None
    return Stage("fetch", True, f"{len(frame)} row(s) for {start}..{end or 'now'}"), frame


def _stage_frame(frame: pd.DataFrame, group: str, source: str, symbol: str,
                 start: str, end: str | None, interval: str) -> Stage:
    """Does the result satisfy its group's contract?"""
    from qde.verify import verify_frame

    violations = verify_frame(
        frame, group, source, symbol, start=start, end=end, interval=interval
    )
    errors = [v for v in violations if v.severity == "error"]
    if errors:
        return Stage("frame", False, "; ".join(f"{v.check}: {v.detail}" for v in errors[:3]))
    if violations:
        # Warnings are recorded, not fatal: a plausibility warning on a genuinely
        # volatile instrument is information, not a defect.
        return Stage("frame", True, f"contract met ({len(violations)} warning(s))")
    return Stage("frame", True, "contract met")


def _stage_determinism(ingestor, symbol: str, start: str, end: str | None,
                       interval: str, first: pd.DataFrame) -> Stage:
    """Ask the identical question twice and require the identical answer.

    The stage that catches what review cannot: pagination keyed to wall-clock time,
    an unstable sort, a cursor that skips a boundary row, a cache serving a different
    page. Each produces a frame that is individually perfect.
    """
    try:
        second = ingestor.load(symbol, start=start, end=end, interval=interval)
    except Exception as exc:
        return Stage("determinism", False, f"second pull raised {type(exc).__name__}: {exc}")

    if len(first) != len(second):
        return Stage(
            "determinism", False,
            f"two identical pulls returned {len(first)} and {len(second)} rows",
        )
    if not first.index.equals(second.index):
        differing = first.index.symmetric_difference(second.index)
        return Stage(
            "determinism", False,
            f"{len(differing)} date(s) differ between two identical pulls "
            f"(e.g. {differing[0] if len(differing) else '?'})",
        )
    shared = [c for c in first.columns if c in second.columns]
    drift = [
        c for c in shared
        if not pd.to_numeric(first[c], errors="coerce").equals(
            pd.to_numeric(second[c], errors="coerce")
        )
    ]
    if drift:
        return Stage("determinism", False, f"values differ between pulls in: {', '.join(drift)}")
    return Stage("determinism", True, "two identical pulls agreed exactly")


def _stage_range(ingestor, symbol: str, interval: str, frame: pd.DataFrame) -> Stage:
    """Does the source honour the window it was given, or ignore it?

    An ingestor that drops its date parameters still returns real data — often *more*
    of it — so nothing downstream notices. Asked for a narrow window inside history
    already known to exist, a correct one answers narrowly.
    """
    if len(frame) < 6 or not isinstance(frame.index, pd.DatetimeIndex):
        return Stage("range", True, "too few rows to carve a sub-window", skipped=True)

    ordered = frame.index.sort_values()
    lo, hi = ordered[1], ordered[min(3, len(ordered) - 1)]
    want_start, want_end = str(lo.date()), str(hi.date())
    try:
        narrow = ingestor.load(symbol, start=want_start, end=want_end, interval=interval)
    except Exception as exc:
        return Stage("range", False, f"narrow pull raised {type(exc).__name__}: {exc}")

    if narrow.empty:
        return Stage("range", False, f"asked for {want_start}..{want_end} and got nothing")
    outside = narrow.index[(narrow.index < lo) | (narrow.index > hi)]
    if len(outside):
        return Stage(
            "range", False,
            f"asked for {want_start}..{want_end} but returned {len(outside)} row(s) "
            f"outside it (e.g. {outside[0].date()}) — the date parameters are ignored",
        )
    return Stage("range", True, f"honoured a narrow {want_start}..{want_end} window")


def _stage_pagination(spec: SourceSpec, frame: pd.DataFrame) -> Stage:
    """Did a window wider than one page actually walk?

    A fetcher that returns the first page and reports success looks identical to one
    that finished, until the day someone asks for more history than a page holds.
    """
    cap = spec.max_rows_per_call
    if cap is None:
        return Stage("pagination", True,
                     "source returns its whole range in one call; nothing to page",
                     skipped=True)
    if len(frame) <= cap:
        return Stage(
            "pagination", True,
            f"{len(frame)} row(s) fits one {cap}-row page, so the walk was never "
            "exercised — widen the window to prove it",
            skipped=True,
        )
    return Stage("pagination", True, f"walked {len(frame)} rows past a {cap}-row page limit")


# Against a PEER the bar is tight, because measurement says it can be. Across eight
# honest pairs in this lake — tiingo vs yfinance on SPY/QQQ/GLD/TLT, and binance vs
# bybit/okx/kucoin/coinbase on BTCUSDT, up to 4,184 dates each — the date sets agree
# to **0.00%**. A peer knows which days actually traded, so holidays are not an
# excuse here; they are already absent from both sides. 1% leaves room for a
# venue-specific halt without letting a systematic drop through, and the first
# version's 5% would have passed an ingestor quietly discarding one row in twenty.
_MAX_MISSING_VS_PEER = 0.01

# The CALENDAR fallback has to be looser: weekdays include market holidays, roughly
# ten a year, which are legitimately absent — a few percent of any window.
_MAX_MISSING_VS_CALENDAR = 0.12


def _stage_completeness(
    frame, group: str, source: str, symbol: str, interval: str,
    start: str, end: str | None,
) -> Stage:
    """Does the frame contain the rows it should, or only correct ones?

    The defect class every other stage is blind to. A deterministic ingestor that
    silently drops every fifth bar returns nothing but real values, honours its date
    range, agrees with itself on a second pull, and matches a peer perfectly — because
    a peer comparison only ever sees the dates BOTH sides have. Six stages inspected
    what was returned; none asked what was missing.

    Two ways to see a hole, strongest first:

    1. **Against a peer's calendar.** Another source covering the same window knows
       which days traded. Dates the peer has and the candidate does not are holes,
       and this catches them even when every returned value is impeccable.
    2. **Against the business calendar.** With no peer, weekdays are the fallback.
       Looser, because market holidays are legitimately absent — roughly ten a year,
       so a few percent of any window.
    """
    if group != "bars" or frame.empty or not isinstance(frame.index, pd.DatetimeIndex):
        return Stage("completeness", True, "not a dated bars frame", skipped=True)
    if interval != "1d":
        return Stage("completeness", True, "only daily bars have a known calendar",
                     skipped=True)

    # The REQUESTED window, not the frame's own span. Measuring against what came
    # back makes truncation invisible: a draft asked for six months and returning
    # three weeks reports "covers 22/22" and passes, which is the same deterministic
    # partial history this stage exists to catch, just a different shape.
    lo = pd.Timestamp(start, tz="UTC")
    hi = pd.Timestamp(end, tz="UTC") if end else frame.index.max()
    ours = set(frame.index.normalize())

    from qde.registry import declared_series

    peers = sorted(
        {
            src
            for src, sym, iv in declared_series(group="bars")
            if sym == symbol and iv == interval and src != source
        }
    )
    # The peer with the MOST dates, not the first that answers. A reference is only
    # as good as its own coverage, and comparing against a peer that is itself short
    # would quietly license the same gap in the candidate.
    best_peer: str | None = None
    best_dates: set = set()
    for peer in peers:
        try:
            from qde.loaders import load_ohlcv

            other = load_ohlcv(
                symbol, start=str(lo.date()), end=str(hi.date()),
                interval=interval, source=peer,
            )
        except Exception:
            continue
        if other.empty or not isinstance(other.index, pd.DatetimeIndex):
            continue
        theirs = {d for d in other.index.normalize() if lo <= d <= hi}
        if len(theirs) > len(best_dates):
            best_peer, best_dates = peer, theirs

    if best_peer is not None and len(best_dates) >= 20:
        peer, theirs = best_peer, best_dates
        missing = sorted(theirs - ours)
        share = len(missing) / len(theirs)
        if share > _MAX_MISSING_VS_PEER:
            return Stage(
                "completeness", False,
                f"{len(missing)} of {len(theirs)} dates that {peer} reports for this "
                f"window are absent ({share:.0%}, e.g. {missing[0].date()}) — the "
                "values returned are fine; it is the rows that are not there",
            )
        return Stage(
            "completeness", True,
            f"covers {len(theirs) - len(missing)}/{len(theirs)} of the dates {peer} "
            "reports",
        )

    # No peer answered: fall back to the weekday calendar over the REQUESTED window.
    weekdays = {d for d in pd.bdate_range(lo, hi, tz=frame.index.tz)}
    if len(weekdays) < 20:
        return Stage("completeness", True, "window too short to judge coverage",
                     skipped=True)
    missing = sorted(weekdays - ours)
    share = len(missing) / len(weekdays)
    if share > _MAX_MISSING_VS_CALENDAR:
        # Without a peer this cannot distinguish a truncating ingestor from an
        # instrument that did not exist for the whole window — so it names both
        # rather than asserting the one it cannot prove. At authoring time a frame
        # this far short of what was asked for is worth stopping on either way.
        return Stage(
            "completeness", False,
            f"{len(missing)} of {len(weekdays)} weekdays in the requested window are "
            f"absent ({share:.0%}, e.g. {missing[0].date()}) — either rows are being "
            "dropped, or the instrument did not trade for the whole window. No peer "
            "was available to tell the two apart",
        )
    return Stage(
        "completeness", True,
        f"{len(weekdays) - len(missing)}/{len(weekdays)} weekdays present "
        "(no peer available; judged against the business calendar)",
    )


def _stage_cross_source(frame: pd.DataFrame, group: str, source: str, symbol: str,
                        interval: str) -> Stage:
    """Where someone else already carries this symbol, do the numbers agree?

    The only stage that can catch a frame which is internally flawless and simply
    wrong — the wrong ticker, the wrong units, an index instead of the ETF.
    """
    if group != "bars":
        return Stage("cross_source", True, "only bars have cross-source peers", skipped=True)

    from qde.verify import cross_check

    try:
        violations = cross_check(frame, symbol, source, interval)
    except Exception as exc:
        return Stage("cross_source", True,
                     f"could not compare against a peer ({type(exc).__name__}: {exc})",
                     skipped=True)

    errors = [v for v in violations if v.severity == "error"]
    if errors:
        return Stage("cross_source", False, errors[0].detail)
    if violations:
        # "No peer carries this symbol" is unverifiable, not agreement. Reported as a
        # pass, it let a frame missing a fifth of its history read as corroborated.
        return Stage("cross_source", True, violations[0].detail, skipped=True)
    return Stage("cross_source", True, "agreed with an independent source")


def run_stages_in_process(
    module_path: str | Path,
    spec: SourceSpec,
    symbol: str,
    start: str,
    end: str | None = None,
    interval: str = "1d",
) -> GauntletReport:
    """Run every stage IN THIS PROCESS. Prefer :func:`run_gauntlet`.

    Public because it is what executes inside the sandbox container, and because the
    project's own already-trusted ingestors are verified with it directly. Calling it
    on a draft you did not write executes that draft as you.

    Args:
        module_path: the drafted module, normally under :data:`QUARANTINE`.
        spec: the registry entry it will be declared with. Not registered by this
            call — a draft must not become fetchable by passing a test.
        symbol / start / end / interval: a window known to hold data.

    Returns:
        A :class:`GauntletReport`. Every stage carries a structured verdict so a
        drafting agent can read its own failures and iterate without a person
        reading the code.
    """
    report = GauntletReport(source=spec.name, symbol=symbol, group=spec.group)

    # No screen here: `run_gauntlet` screens on the host before a container is even
    # started, and the in-container path is reached only after it passed.
    stage, cls = _stage_contract(module_path)
    report.stages.append(stage)
    if not stage.passed or cls is None:
        return report

    ingestor = cls(spec)

    # Every stage below calls into candidate code. It runs holding only the one
    # credential this source legitimately needs — a tiingo draft cannot casually read
    # FRED's key or the R2 read keys out of the environment.
    with _only_this_sources_credentials(spec.name):
        stage, frame = _stage_fetch(ingestor, symbol, start, end, interval)
        report.stages.append(stage)
        if not stage.passed or frame is None:
            return report

        report.stages.append(
            _stage_frame(frame, spec.group, spec.name, symbol, start, end, interval)
        )
        report.stages.append(_stage_determinism(ingestor, symbol, start, end, interval, frame))
        report.stages.append(_stage_range(ingestor, symbol, interval, frame))
        report.stages.append(_stage_pagination(spec, frame))
        report.stages.append(
            _stage_completeness(
                frame, spec.group, spec.name, symbol, interval, start, end
            )
        )

    # Outside the scope: this one compares against an ALREADY-TRUSTED source, so it
    # needs that source's credentials rather than the candidate's.
    report.stages.append(_stage_cross_source(frame, spec.group, spec.name, symbol, interval))
    return report


_TEMPLATE = '''"""DRAFT ingestor for {name} ({group}).

Generated by `qde.draft scaffold`. NOT registered and NOT importable by the
pipeline — it lives in quarantine until it passes the gauntlet and a person moves
it into `src/qde/ingest/`.

Prove it with:

    python -m qde.draft verify {path} --name {name} --symbol <SYMBOL> --from <YYYY-MM-DD>

The gauntlet checks what a code review cannot: that two identical pulls agree, that
the date range is honoured rather than ignored, and that the numbers match an
independent source where one exists.
"""

from typing import Any

import pandas as pd

from qde.ingest.base import BaseIngestor, RawPage
from qde.loaders.http import get_with_requests


class {cls}(BaseIngestor):
    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> Any:
        # Where the walk begins: a start date, an epoch millisecond, a page token.
        # Return the value `fetch_page` expects as its `cursor`.
        raise NotImplementedError

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        # ONE page. `next_cursor=None` ends the walk; a single-shot API returns None
        # after its only page. Use `get_with_requests` so retry/backoff and the
        # connect/read timeout are applied — a bare `requests.get` can hang forever.
        raise NotImplementedError

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        # The accumulated raw rows -> the canonical frame for group={group!r}:
        #   bars   : UTC `date` index + open/high/low/close/volume
        #   series : UTC `date` index + `value`, or one column per metric
        #   events : event_id, revision_seq, scheduled_ts, observed_ts
        #
        # Two things that pass review and fail the gauntlet:
        #   - reading an epoch in the wrong unit (seconds vs milliseconds) builds a
        #     valid index pointing at 1970 or the year 55000;
        #   - mapping an adjusted close onto `close` gives plausible numbers that
        #     break `high >= close`.
        raise NotImplementedError
'''


DOCKER_IMAGE = "qde-collector"


def _docker_available() -> bool:
    import subprocess  # noqa: S404 - invoking the local docker client, not user input

    try:
        return (
            subprocess.run(
                ["docker", "info"], capture_output=True, timeout=20, check=False
            ).returncode
            == 0
        )
    except Exception:
        return False


def _run_in_container(
    module_path: Path, spec: SourceSpec, symbol: str, start: str,
    end: str | None, interval: str,
) -> GauntletReport:
    """Execute the gauntlet inside a disposable container and bring the report back.

    What this actually contains, and why each part is there:

    - **A throwaway filesystem.** ``--rm`` and no bind mount except the draft itself,
      read-only. A hostile draft cannot reach ``secrets/``, because the directory is
      simply not there — unlike a subprocess, which would run as the same user with
      the same filesystem and contain nothing at all.
    - **One credential.** Only the source's own key is passed through, so a compromise
      costs the secret the draft was already trusted with and nothing else.
    - **Network, deliberately.** The whole point is to fetch from a real API, so the
      container has to reach the internet. Exfiltration is therefore still possible —
      what the sandbox removes is access to anything worth exfiltrating.

    Isolation is the DEFAULT rather than a flag on purpose. Requiring a human to
    approve each execution would put the reviewer back in the loop that this module
    exists to remove: an agent iterating against the gauntlet would either need a
    person for every attempt, or would pass the flag itself, which is theatre.
    """
    import json
    import subprocess  # noqa: S404 - fixed argv, no shell

    key = f"{spec.name.upper()}_API_KEY"
    argv = [
        "docker", "run", "--rm",
        # Unprivileged. The image has no USER directive, so without this the
        # candidate would run as root inside the container — and root plus default
        # capabilities is the starting position for most container escapes.
        "--user", "65534:65534",
        "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        # Nothing on disk survives, and nothing can be written except scratch space.
        # pandas and pyarrow need a writable temp dir, so /tmp is a tmpfs rather than
        # the image filesystem, and HOME points at it for libraries that cache there.
        "--read-only",
        "--tmpfs", "/tmp:rw,noexec,nosuid,size=256m",
        "-e", "HOME=/tmp",
        # A hostile or merely buggy draft must not be able to take the box down —
        # this host also runs the 24/7 collectors.
        "--pids-limit", "256",
        "--memory", "2g",
        # Network stays ON: the whole point is fetching from a real API, so
        # exfiltration remains possible. What the sandbox removes is anything worth
        # exfiltrating — no secrets mount, one credential, no persistence.
        "--network", "bridge",
        "-v", f"{module_path.resolve().as_posix()}:/draft/candidate.py:ro",
        "-e", key,
        DOCKER_IMAGE,
        "python", "-m", "qde.draft", "_stages",
        "/draft/candidate.py", "--name", spec.name, "--group", spec.group,
        "--symbol", symbol, "--native", spec.symbols.get(symbol, symbol),
        "--from", start, "--interval", interval,
    ]
    if end:
        argv += ["--to", end]
    if spec.max_rows_per_call:
        argv += ["--max-rows-per-call", str(spec.max_rows_per_call)]

    env = dict(os.environ)
    env.setdefault(key, os.environ.get(key, ""))
    done = subprocess.run(argv, capture_output=True, text=True, timeout=900, env=env)

    payload = next(
        (ln for ln in reversed(done.stdout.splitlines()) if ln.startswith("{")), None
    )
    if payload is None:
        return GauntletReport(
            source=spec.name, symbol=symbol, group=spec.group,
            stages=[Stage("sandbox", False,
                          f"the sandboxed run produced no report (exit {done.returncode}): "
                          f"{(done.stderr or done.stdout)[-300:]}", blocking=True)],
        )

    data = json.loads(payload)
    return GauntletReport(
        source=data["source"], symbol=data["symbol"], group=data["group"],
        stages=[Stage(**st) for st in data["stages"]],
    )


def run_gauntlet(
    module_path: str | Path,
    spec: SourceSpec,
    symbol: str,
    start: str,
    end: str | None = None,
    interval: str = "1d",
    isolation: str = "container",
) -> GauntletReport:
    """Prove a drafted ingestor, running it in a disposable container by default.

    Args:
        isolation: ``"container"`` (default) executes the candidate in a throwaway
            container with no ``secrets/`` mount and only its own credential.
            ``"in-process"`` runs it here — correct for this project's own ingestors
            and for tests, and unsafe for anything you did not write.

    The AST screen runs on the host first, so an obviously hostile draft is refused
    before a container is even started.
    """
    module_path = Path(module_path)
    report = GauntletReport(source=spec.name, symbol=symbol, group=spec.group)

    try:
        smells = screen_source(module_path)
    except SyntaxError as exc:
        report.stages.append(Stage("screen", False, f"will not parse: {exc}", blocking=True))
        return report
    if smells:
        report.stages.append(
            Stage("screen", False,
                  "an ingestor needs HTTP, pandas and the registry, nothing else — "
                  + "; ".join(smells[:4]), blocking=True))
        return report

    if isolation == "in-process":
        result = run_stages_in_process(module_path, spec, symbol, start, end, interval)
        result.stages.insert(0, Stage("screen", True, "no forbidden imports or calls"))
        return result

    if not _docker_available():
        # Failing is the honest outcome. Quietly falling back to in-process would
        # execute an unvetted draft as you at the exact moment the safety mechanism
        # was unavailable — the worst possible time to degrade silently.
        report.stages.append(
            Stage("sandbox", False,
                  "docker is not available, so the candidate cannot be contained. "
                  "Start docker, or pass isolation='in-process' if you wrote this "
                  "draft yourself and accept running it as you.", blocking=True))
        return report

    result = _run_in_container(module_path, spec, symbol, start, end, interval)
    result.stages.insert(0, Stage("screen", True, "no forbidden imports or calls"))
    return result


def scaffold(spec: SourceSpec, directory: str | Path = QUARANTINE) -> Path:
    """Write a skeleton ingestor for ``spec`` into quarantine and return its path.

    The skeleton exists so a generator starts from this project's pattern — the
    pagination loop, the retrying HTTP helper, the canonical frame shape — rather
    than inventing its own and being marked down for it by the gauntlet.
    """
    out_dir = Path(directory)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{spec.name}_{spec.group}.py"
    cls = "".join(part.capitalize() for part in spec.name.split("_")) + "Ingestor"
    path.write_text(
        _TEMPLATE.format(
            name=spec.name, group=spec.group, cls=cls, path=path.as_posix()
        ),
        encoding="utf-8",
    )
    return path


def main() -> None:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="python -m qde.draft",
        description="Scaffold a new source's ingestor, then prove it against the live API.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    s = sub.add_parser("scaffold", help="write a skeleton ingestor into quarantine")
    s.add_argument("--name", required=True, help="source name, e.g. tiingo")
    s.add_argument("--group", required=True, choices=["bars", "series", "events"])
    s.add_argument("--symbol", action="append", default=[], metavar="CANON=NATIVE",
                   help="symbol mapping, repeatable")

    v = sub.add_parser("verify", help="run a drafted ingestor through the gauntlet")
    v.add_argument("module", help="path to the drafted module")
    v.add_argument("--name", required=True)
    v.add_argument("--group", default="bars", choices=["bars", "series", "events"])
    v.add_argument("--symbol", required=True)
    v.add_argument("--native", help="source-native symbol (defaults to --symbol)")
    v.add_argument("--from", dest="start", required=True)
    v.add_argument("--to", dest="end")
    v.add_argument("--interval", default="1d")
    v.add_argument("--max-rows-per-call", type=int)

    # Hidden: what runs INSIDE the sandbox container. Emits the report as JSON on
    # stdout so the host can reconstruct it without trusting anything else the
    # container printed.
    st = sub.add_parser("_stages")
    st.add_argument("module")
    st.add_argument("--name", required=True)
    st.add_argument("--group", default="bars")
    st.add_argument("--symbol", required=True)
    st.add_argument("--native")
    st.add_argument("--from", dest="start", required=True)
    st.add_argument("--to", dest="end")
    st.add_argument("--interval", default="1d")
    st.add_argument("--max-rows-per-call", type=int)
    v.add_argument(
        "--in-process", action="store_true",
        help="run the candidate in THIS process instead of a disposable container. "
             "Correct for ingestors you wrote; unsafe for anything you did not.",
    )

    args = parser.parse_args()

    if args.command == "scaffold":
        mapping = dict(pair.split("=", 1) for pair in args.symbol) if args.symbol else {}
        spec = SourceSpec(
            group=args.group, name=args.name, symbols=mapping, intervals=["1d"],
            max_rows_per_call=None, rate_limit_per_min=None, expected_daily_rows=1,
            null_tolerance={},
            # Never inferred. Whether data may be republished is a licensing judgment
            # with legal consequences, not a property measurable from a response.
            redistributable=False,
            license_note="DRAFT — licensing not reviewed; redistributable stays False "
                         "until a person checks the source's terms.",
        )
        path = scaffold(spec)
        print(f"scaffolded {path}")
        print("fill in the three methods, then run `python -m qde.draft verify ...`")
        return

    native = args.native or args.symbol
    spec = SourceSpec(
        group=args.group, name=args.name, symbols={args.symbol: native}, intervals=[args.interval],
        max_rows_per_call=args.max_rows_per_call, rate_limit_per_min=None,
        expected_daily_rows=1, null_tolerance={}, redistributable=False,
        license_note="DRAFT",
    )
    if args.command == "_stages":
        import dataclasses
        import json

        report = run_stages_in_process(
            args.module, spec, args.symbol, args.start, args.end, args.interval
        )
        print(json.dumps(dataclasses.asdict(report)))
        sys.exit(0 if report.passed else 1)

    report = run_gauntlet(
        args.module, spec, args.symbol, args.start, args.end, args.interval,
        isolation="in-process" if args.in_process else "container",
    )
    print(report.summary())
    # Non-zero on failure so a drafting agent, or CI, can branch on the result
    # without parsing prose.
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
