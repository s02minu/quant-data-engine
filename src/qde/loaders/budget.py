"""A request budget that survives the process it was spent in.

``rate_limit_per_min`` paces a single run: it spaces calls so one loop cannot
hammer a source. That is all it can do, because its state is a dict in one Python
process. A second command, a second container, or the gauntlet running a candidate
ingestor each start with a clean budget the source does not grant them.

Tiingo's free tier is the case that makes the difference matter: **50 requests per
hour, 1000 per day** (measured from their pricing page, not assumed). A nightly pass
over 27 symbols fits comfortably. Two passes in the same hour do not — and nothing
stopped the second one, because nothing carried the first one's spend forward.

So the budget is a file, not a variable.

**Fixed windows, not sliding.** Tiingo documents "Hourly Requests - Reset every
hour", so the allowance refills at the top of the hour rather than trailing the last
sixty minutes. A sliding window would be stricter than the source actually is and
would stall a run that the API would have served.

**Reserve, then check.** The slot is appended *before* the count is examined. Two
processes racing would otherwise both read 49, both append, and both proceed. The
cost of this ordering is that a rejected call still consumed its slot; the benefit is
that the limiter can never let more through than the source allows. For a limiter
that is the correct direction to be wrong in.

**Append-only ledger.** One short line per attempt, written with ``O_APPEND``, which
is atomic for writes this size. Counting is then just reading lines in the current
window -- no lock file, no read-modify-write to lose.
"""

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path


class RateBudgetExhausted(RuntimeError):
    """Raised when a source's declared hourly allowance is already spent.

    Deliberately loud rather than a long sleep. The wait to the top of the hour can
    be fifty-nine minutes, and a nightly that silently blocks for an hour looks
    identical to a nightly that hung -- the failure shape this platform works hardest
    to avoid. The message carries the spend and the reset time so the operator knows
    exactly when to try again.
    """


def _state_dir() -> Path:
    """Where the ledgers live: a directory every container shares.

    ``QDE_STATE_DIR`` wins when set. Otherwise ``/data`` is used when it exists,
    because that is the one path ``docker-compose.yml`` mounts into every service --
    a budget written to the image's own filesystem would vanish with the container
    and share nothing, which is the exact defect this module exists to fix. Falling
    back to ``data/.state`` keeps a laptop run working with no configuration.

    The leading dot keeps it out of the Hive partitions; the publish and sync globs
    match ``**/*.parquet``, so nothing here is ever uploaded.
    """
    explicit = os.getenv("QDE_STATE_DIR")
    if explicit:
        return Path(explicit)
    if Path("/data").is_dir():
        return Path("/data/.state")
    return Path("data/.state")


def _ledger(source: str) -> Path:
    return _state_dir() / "ratelimit" / f"{source}.txt"


def _hourly_limit(source: str) -> int | None:
    """The source's declared hourly allowance, or ``None`` if it declares none.

    Read from the registry rather than passed in, so the limit cannot drift from the
    row that documents it. An unregistered name -- a drafted candidate under a
    working title -- simply has no budget to enforce.
    """
    from qde.registry import all_specs

    for spec in all_specs():
        if spec.name == source:
            return spec.rate_limit_per_hour
    return None


def consume(source: str | None, *, now: float | None = None) -> None:
    """Record one request against ``source`` and refuse it if the hour is spent.

    A no-op for a source with no declared hourly limit, which is every venue that
    pages freely -- this costs them one registry lookup and no disk access.

    Args:
        source: the registered source name, or ``None`` when the caller has no
            source context (a bare URL fetch), in which case nothing is metered.
        now: unix time, injectable so the tests can cross an hour boundary without
            waiting for one.

    Raises:
        RateBudgetExhausted: when this request would exceed the declared allowance.
    """
    if not source:
        return
    limit = _hourly_limit(source)
    if not limit or limit <= 0:
        return

    stamp = time.time() if now is None else now
    window = int(stamp // 3600)
    path = _ledger(source)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Reserve first -- see the module docstring on ordering.
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"{window} {stamp:.3f}\n")

    spent = _spend_in_window(path, window)
    if spent > limit:
        resets_at = (window + 1) * 3600
        raise RateBudgetExhausted(
            f"{source} has used {spent} of its {limit} requests this hour; the "
            f"allowance resets at {time.strftime('%H:%M:%SZ', time.gmtime(resets_at))} "
            f"({max(int(resets_at - stamp), 0)}s). Re-running now would earn 429s "
            "rather than data."
        )


def _spend_in_window(path: Path, window: int) -> int:
    """Count this window's entries, pruning the file when older ones dominate.

    Pruning is opportunistic rather than scheduled: rewriting only once the stale
    entries outnumber the live ones keeps the common path a plain read, and bounds
    the file at roughly twice one hour's traffic.
    """
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return 0

    live = [line for line in lines if line.startswith(f"{window} ")]
    if len(lines) > 2 * max(len(live), 1):
        tmp = path.with_suffix(".tmp")
        tmp.write_text("".join(f"{line}\n" for line in live), encoding="utf-8")
        os.replace(tmp, path)
    return len(live)


def spent_this_hour(source: str, *, now: float | None = None) -> int:
    """This hour's recorded spend. For reporting and tests; records nothing."""
    stamp = time.time() if now is None else now
    return _spend_in_window(_ledger(source), int(stamp // 3600))


# --- which source the current fetch belongs to -------------------------------------

# A ContextVar rather than an argument threaded through ten call sites. Each concrete
# `fetch_page` calls `get_with_requests` itself, so passing the source explicitly would
# mean editing every ingestor and trusting that the next one remembers -- the same
# "reads like a control, behaves like a comment" trap `rate_limit_per_min` fell into.
# `BaseIngestor` sets this once around the fetch, so a source added tomorrow is metered
# without touching this module or the HTTP layer.
_current_source: ContextVar[str | None] = ContextVar("qde_current_source", default=None)


@contextmanager
def fetching(source: str) -> Iterator[None]:
    """Mark the HTTP calls made inside this block as belonging to ``source``."""
    token = _current_source.set(source)
    try:
        yield
    finally:
        _current_source.reset(token)


def current_source() -> str | None:
    """The source whose fetch is in progress, or ``None`` outside one."""
    return _current_source.get()
