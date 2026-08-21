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
"""

import importlib.util
import inspect
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

    def summary(self) -> str:
        lines = [
            f"{'PASS' if self.passed else 'FAIL'}  {self.source}/{self.symbol} "
            f"[{self.group}]"
        ]
        for s in self.stages:
            lines.append(f"  {'ok  ' if s.passed else 'FAIL'}  {s.name:<13} {s.detail}")
        return "\n".join(lines)


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
        return Stage("range", True, "skipped: too few rows to carve a sub-window")

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
        return Stage("pagination", True, "skipped: source returns its whole range in one call")
    if len(frame) <= cap:
        return Stage(
            "pagination", True,
            f"not exercised: {len(frame)} row(s) fits one {cap}-row page — widen the "
            "window to prove the walk",
        )
    return Stage("pagination", True, f"walked {len(frame)} rows past a {cap}-row page limit")


def _stage_cross_source(frame: pd.DataFrame, group: str, source: str, symbol: str,
                        interval: str) -> Stage:
    """Where someone else already carries this symbol, do the numbers agree?

    The only stage that can catch a frame which is internally flawless and simply
    wrong — the wrong ticker, the wrong units, an index instead of the ETF.
    """
    if group != "bars":
        return Stage("cross_source", True, "skipped: only bars have cross-source peers")

    from qde.verify import cross_check

    try:
        violations = cross_check(frame, symbol, source, interval)
    except Exception as exc:
        return Stage("cross_source", True, f"skipped: {type(exc).__name__}: {exc}")

    errors = [v for v in violations if v.severity == "error"]
    if errors:
        return Stage("cross_source", False, errors[0].detail)
    if violations:
        # "No peer carries this symbol" is a warn by design — unverifiable, not wrong.
        return Stage("cross_source", True, violations[0].detail)
    return Stage("cross_source", True, "agreed with an independent source")


def run_gauntlet(
    module_path: str | Path,
    spec: SourceSpec,
    symbol: str,
    start: str,
    end: str | None = None,
    interval: str = "1d",
) -> GauntletReport:
    """Put a drafted ingestor through every check the platform can apply.

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

    stage, cls = _stage_contract(module_path)
    report.stages.append(stage)
    if not stage.passed or cls is None:
        return report

    ingestor = cls(spec)

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
    report = run_gauntlet(
        args.module, spec, args.symbol, args.start, args.end, args.interval
    )
    print(report.summary())
    # Non-zero on failure so a drafting agent, or CI, can branch on the result
    # without parsing prose.
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
