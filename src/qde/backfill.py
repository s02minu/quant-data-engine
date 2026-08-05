"""Group-level backfill for the bars lake.

Re-pull OHLCV history for one or more series over a date range and merge it into
the bronze bars lake. Every write goes through ``qde.storage.upsert_bars``, so a
backfill is idempotent: an overlapping or repeated run converges to one row per
date instead of accumulating duplicates.

The command is *group-level* -- it targets the ``bars`` group and works
identically across sources, because every source in the group writes the same
schema against the same key. The set of series to backfill is either named
explicitly on the command line, enumerated from the source registry with
``--from-registry`` (the declared/intended set, so a series that is not yet in
the lake can be seeded), or discovered from what is already in the lake.

Example:
    python -m qde.backfill --source binance --symbol BTCUSDT --from 2020-01-01
    python -m qde.backfill --source binance --from 2020-01-01   # binance series in the lake
    python -m qde.backfill --from-registry --from 2020-01-01    # every declared series (seeds new)
    python -m qde.backfill --from 2020-01-01                    # every series in the lake
"""

import os

from qde.loaders import load_ohlcv
from qde.log import configure, get_logger
from qde.registry import declared_series
from qde.storage import list_bars_series, upsert_bars

log = get_logger(__name__)

Series = tuple[str, str, str]  # (symbol, source, interval)


def backfill_series(
    symbol: str,
    source: str,
    interval: str,
    start: str,
    end: str | None = None,
    base_dir: str = "data",
) -> int:
    """Fetch ``[start, end]`` for one series and upsert it into the lake.

    Returns:
        int: the resulting row count for the series.
    """
    df = load_ohlcv(symbol, start=start, end=end, interval=interval, source=source)
    return upsert_bars(df, symbol, source, interval, base_dir)


def _resolve_series(
    source: str | None,
    symbol: str | None,
    interval: str | None,
    base_dir: str,
    use_registry: bool = False,
) -> list[Series]:
    """Decide which series a backfill should touch.

    A fully specified ``source`` + ``symbol`` names a single series directly and
    need not exist yet -- this is how a brand-new series is bootstrapped.
    Otherwise a candidate set is enumerated -- the registry's declared series
    when ``use_registry`` is set (the intended full set, so a declared-but-
    unseeded series is included), or the series already in the lake by default --
    and narrowed by whichever of ``source`` / ``symbol`` / ``interval`` were given.
    """
    if source and symbol:
        return [(symbol, source, interval or "1d")]

    if use_registry:
        # declared_series yields (source, symbol, interval); Series is
        # (symbol, source, interval).
        candidates = [(sym, src, iv) for (src, sym, iv) in declared_series(group="bars")]
    else:
        df = list_bars_series(base_dir)
        candidates = [
            (str(row.symbol), str(row.source), str(row.interval))
            for row in df.itertuples(index=False)
        ]

    if source:
        candidates = [s for s in candidates if s[1] == source]
    if symbol:
        candidates = [s for s in candidates if s[0] == symbol]
    if interval:
        candidates = [s for s in candidates if s[2] == interval]

    return candidates


def backfill_bars(
    start: str,
    end: str | None = None,
    source: str | None = None,
    symbol: str | None = None,
    interval: str | None = None,
    base_dir: str = "data",
    use_registry: bool = False,
) -> dict[Series, int]:
    """Backfill every matching bar series over ``[start, end]``.

    With no filter, refreshes every series already in the lake; set
    ``use_registry`` to instead enumerate the registry's declared set (so a
    declared-but-unseeded series is seeded). ``source`` / ``symbol`` / ``interval``
    narrow the set, and ``source`` + ``symbol`` together can bootstrap a series
    that is in neither. One series failing (a bad symbol, a source outage) is
    logged and skipped so it does not abort the rest of the run.

    Returns:
        dict[Series, int]: row count per successfully backfilled series, keyed
        by ``(symbol, source, interval)``.
    """
    series = _resolve_series(source, symbol, interval, base_dir, use_registry=use_registry)
    if not series:
        log.warning("backfill_no_series", source=source, symbol=symbol, interval=interval)
        return {}

    results: dict[Series, int] = {}
    for s_symbol, s_source, s_interval in series:
        try:
            rows = backfill_series(s_symbol, s_source, s_interval, start, end, base_dir)
        except Exception as exc:
            log.warning(
                "backfill_failed",
                symbol=s_symbol,
                source=s_source,
                interval=s_interval,
                error=type(exc).__name__,
                detail=str(exc),
            )
            continue

        results[(s_symbol, s_source, s_interval)] = rows
        log.info("backfilled", symbol=s_symbol, source=s_source, interval=s_interval, rows=rows)

    return results


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m qde.backfill",
        description="Idempotently backfill OHLCV bars over a date range.",
    )
    parser.add_argument(
        "--group", default="bars", help="data group to backfill (only 'bars' for now)"
    )
    parser.add_argument("--source", help="restrict to one source, e.g. binance")
    parser.add_argument("--symbol", help="restrict to one symbol, e.g. BTCUSDT")
    parser.add_argument("--interval", help="restrict to one bar size, e.g. 1d")
    parser.add_argument(
        "--from-registry",
        dest="use_registry",
        action="store_true",
        help="enumerate the registry's declared series (the intended set) instead "
        "of the lake, so a declared-but-unseeded series gets seeded",
    )
    parser.add_argument("--from", dest="start", required=True, help="start date, e.g. 2020-01-01")
    parser.add_argument("--to", dest="end", default=None, help="end date (default: now)")
    parser.add_argument(
        "--base-dir",
        default=os.getenv("QDE_BASE_DIR", "data"),
        help="lake root (default: $QDE_BASE_DIR, else 'data') -- matches compact/sync",
    )
    args = parser.parse_args()

    if args.group != "bars":
        parser.error(f"unsupported group {args.group!r}; only 'bars' is available for now")

    configure()
    results = backfill_bars(
        start=args.start,
        end=args.end,
        source=args.source,
        symbol=args.symbol,
        interval=args.interval,
        base_dir=args.base_dir,
        use_registry=args.use_registry,
    )
    log.info(
        "backfill_complete",
        series=len(results),
        total_rows=sum(results.values()),
    )


if __name__ == "__main__":
    main()
