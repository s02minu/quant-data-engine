"""Unified OHLCV loading facade.

``load_ohlcv`` is the one public entry point for pulling a bar series: it resolves
the source from the registry, rejects an unknown source or an unmapped symbol
*before* any network call, then delegates to that source's ingestor. The
per-source fetch/pagination/normalize logic lives in ``qde.ingest``; the retry
helper and the ``NoNewData`` sentinel stay here because the stream collector and
storage layer already import them from this package.
"""

import pandas as pd

from qde.loaders.exceptions import NoNewData

__all__ = ["NoNewData", "load_ohlcv"]


def load_ohlcv(
    symbol: str, start: str, end: str | None = None, interval: str = "1d", source: str = "yfinance"
) -> pd.DataFrame:
    """Fetch a bar series for a canonical symbol from ``source``.

    Args:
        symbol: A canonical uppercase symbol, e.g. ``"BTCUSDT"``.
        start: Start date, e.g. ``"2020-01-01"``.
        end: End date; defaults to now.
        interval: Bar size, e.g. ``"1d"``.
        source: Registered source name, e.g. ``"binance"``.

    Returns:
        A cleaned OHLCV DataFrame: ``open/high/low/close/volume`` columns on a
        UTC-aware ``date`` index.

    Raises:
        ValueError: if ``source`` is not registered, or ``symbol`` is not one the
            source provides — both caught before any network call.
        NoNewData: if the source has no rows in range (a ``ValueError`` subclass).
    """
    # Imported lazily: qde.ingest imports from this package, so importing it at
    # module load would form a cycle. By the time load_ohlcv is called, both
    # packages are fully initialized.
    from qde.ingest import get_ingestor
    from qde.registry import SOURCES, get_spec

    try:
        spec = get_spec(source)
    except KeyError as exc:
        supported = ", ".join(sorted(SOURCES))
        raise ValueError(
            f"The source {source!r} is not supported. Available sources: {supported}."
        ) from exc

    if symbol not in spec.symbols:
        raise ValueError(f"Unknown symbol {symbol!r} for source {source!r}")

    return get_ingestor(source).load(symbol, start, end=end, interval=interval)
