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

Works across groups: ``--group bars`` (default) or ``--group series``. For the
series group ``--symbol`` is the series id (e.g. ``DGS10``) and a FRED key is
loaded from ``secrets/fred.env`` if not already exported.

Example:
    python -m qde.backfill --source binance --symbol BTCUSDT --from 2020-01-01
    python -m qde.backfill --source binance --from 2020-01-01   # binance bars in the lake
    python -m qde.backfill --from-registry --from 2020-01-01    # every declared bar series
    python -m qde.backfill --from 2020-01-01                    # every bar series in the lake
    python -m qde.backfill --group series --from-registry --from 2010-01-01   # all declared series
    python -m qde.backfill --group series --source fred --symbol DGS10 --from 2010-01-01
"""

import os

from qde.env import load_env_file
from qde.ingest import get_ingestor
from qde.loaders import load_ohlcv
from qde.log import configure, get_logger
from qde.registry import declared_series
from qde.storage import (
    list_bars_series,
    list_series,
    upsert_bars,
    upsert_series_frame,
)

log = get_logger(__name__)

Series = tuple[str, str, str]  # (symbol, source, interval)  -- bars
SeriesId = tuple[str, str]  # (source, series_id)            -- series group


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


# --- series group -----------------------------------------------------------
#
# The scalar-series twin of the bars backfill above. The orchestration is the
# same (resolve a set, fetch + upsert each, skip failures); only the identity
# (source, series_id) and the storage/fetch calls differ.


def _resolve_series_group(
    source: str | None,
    series_id: str | None,
    base_dir: str,
    use_registry: bool,
) -> list[SeriesId]:
    """Decide which ``(source, series_id)`` scalar series a backfill should touch.

    ``source`` + ``series_id`` names one directly (and need not exist yet).
    Otherwise the candidate set is the registry's declared series (``use_registry``)
    or those already in the lake, narrowed by whichever filter was given.
    """
    if source and series_id:
        return [(source, series_id)]

    if use_registry:
        # declared_series yields (source, series_id, interval); drop the unused
        # interval (a bars-ism) for the series group.
        candidates = [(src, sid) for (src, sid, _iv) in declared_series(group="series")]
    else:
        df = list_series(base_dir)
        candidates = [(str(r.source), str(r.series_id)) for r in df.itertuples(index=False)]

    if source:
        candidates = [c for c in candidates if c[0] == source]
    if series_id:
        candidates = [c for c in candidates if c[1] == series_id]

    return candidates


def backfill_series_group(
    start: str,
    end: str | None = None,
    source: str | None = None,
    series_id: str | None = None,
    base_dir: str = "data",
    use_registry: bool = False,
) -> dict[SeriesId, int]:
    """Backfill every matching scalar series over ``[start, end]``.

    The ``series``-group twin of :func:`backfill_bars`: idempotent per date, one
    failing series is logged and skipped. ``use_registry`` seeds declared-but-
    unseeded series (e.g. the full FRED set).

    Returns:
        dict[SeriesId, int]: row count per series, keyed by ``(source, series_id)``.
    """
    series = _resolve_series_group(source, series_id, base_dir, use_registry)
    if not series:
        log.warning("backfill_no_series", group="series", source=source, series_id=series_id)
        return {}

    results: dict[SeriesId, int] = {}
    for s_source, s_series_id in series:
        try:
            df = get_ingestor(s_source).load(s_series_id, start, end)
            rows = upsert_series_frame(df, s_series_id, s_source, base_dir)
        except Exception as exc:
            log.warning(
                "backfill_failed",
                group="series",
                source=s_source,
                series_id=s_series_id,
                error=type(exc).__name__,
                detail=str(exc),
            )
            continue

        results[(s_source, s_series_id)] = rows
        log.info("backfilled", group="series", source=s_source, series_id=s_series_id, rows=rows)

    return results


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m qde.backfill",
        description="Idempotently backfill a data group over a date range.",
    )
    parser.add_argument(
        "--group",
        default="bars",
        choices=["bars", "series"],
        help="data group to backfill",
    )
    parser.add_argument("--source", help="restrict to one source, e.g. binance or fred")
    parser.add_argument(
        "--symbol", help="restrict to one symbol (bars) or series id (series), e.g. BTCUSDT / DGS10"
    )
    parser.add_argument("--interval", help="restrict to one bar size, e.g. 1d (bars only)")
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

    configure()
    if args.group == "bars":
        results: dict = backfill_bars(
            start=args.start,
            end=args.end,
            source=args.source,
            symbol=args.symbol,
            interval=args.interval,
            base_dir=args.base_dir,
            use_registry=args.use_registry,
        )
    else:  # series
        # A series source (FRED) needs its API key; load it from the gitignored
        # secrets file if not already exported.
        load_env_file("secrets/fred.env")
        results = backfill_series_group(
            start=args.start,
            end=args.end,
            source=args.source,
            series_id=args.symbol,  # --symbol is the series id for the series group
            base_dir=args.base_dir,
            use_registry=args.use_registry,
        )

    log.info(
        "backfill_complete",
        group=args.group,
        series=len(results),
        total_rows=sum(results.values()),
    )


if __name__ == "__main__":
    main()
