"""The source registry — the little book of every source the platform knows.

This module *is* the registry: a single place where each source is declared once
as a :class:`SourceSpec`. Everything downstream (ingestor config, the DQ
contract, the public catalogue) reads from here, so a source's facts live in one
place and cannot drift.

Adding a source is meant to be a one-row change: append a ``SourceSpec`` to
``_SPECS`` and implement its ingestor (Phase 4). That is the whole payoff of the
pattern — a new instrument type stops being a new module.
"""

import pandas as pd

from qde.registry.spec import SourceSpec

# OHLCV bars carry no nulls in any price/volume column: a missing value is a
# defect, not a tolerated gap. Shared across the bar sources so the contract is
# written once.
_OHLCV_NO_NULLS: dict[str, float] = {
    "open": 0.0,
    "high": 0.0,
    "low": 0.0,
    "close": 0.0,
    "volume": 0.0,
}

_SPECS: list[SourceSpec] = [
    SourceSpec(
        group="bars",
        name="binance",
        # Identity mapping: Binance already uses the project's canonical spelling.
        symbols={"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT", "SOLUSDT": "SOLUSDT"},
        intervals=["1d"],
        max_rows_per_call=1000,  # the klines endpoint's per-page cap
        rate_limit_per_min=1200,  # request-weight budget; a kline call costs little
        expected_daily_rows=1,
        null_tolerance=_OHLCV_NO_NULLS,
        redistributable=True,
        license_note=(
            "Binance public REST market data (historical klines); exchange-native "
            "and generally redistributable. Verify Binance API terms before public "
            "publishing."
        ),
    ),
    SourceSpec(
        group="bars",
        name="kraken",
        symbols={"BTCUSDT": "XBTUSD", "ETHUSDT": "ETHUSD"},
        intervals=["1d"],
        max_rows_per_call=720,  # Kraken's OHLC endpoint returns at most ~720 candles
        rate_limit_per_min=60,  # conservative for the public tier
        expected_daily_rows=1,
        null_tolerance=_OHLCV_NO_NULLS,
        redistributable=True,
        license_note=(
            "Kraken public REST OHLC data; exchange-native and generally "
            "redistributable. Verify Kraken API terms before public publishing."
        ),
    ),
    SourceSpec(
        group="bars",
        name="yfinance",
        symbols={
            "BTCUSDT": "BTC-USD",
            "ETHUSDT": "ETH-USD",
            "SOLUSDT": "SOL-USD",
            "SPY": "SPY",  # S&P 500 ETF
            "QQQ": "QQQ",  # Nasdaq 100 ETF
            "GLD": "GLD",  # Gold ETF
            "TLT": "TLT",  # 20+ Year Treasury Bond ETF
            "DX-Y.NYB": "DX-Y.NYB",  # US Dollar Index (DXY)
        },
        intervals=["1d"],
        max_rows_per_call=None,  # yfinance returns the whole range in one download
        rate_limit_per_min=None,  # unofficial scrape; no published request limit
        expected_daily_rows=1,  # equities: a market-closed day legitimately adds zero
        null_tolerance=_OHLCV_NO_NULLS,
        redistributable=False,
        license_note=(
            "Scrapes Yahoo Finance; Yahoo's terms prohibit redistribution. "
            "Code-only source — the ingestor is open-sourced, the data is not "
            "published to the public lake."
        ),
    ),
]

# Registry indexed by source name for O(1) lookup. Names are unique by construction.
SOURCES: dict[str, SourceSpec] = {spec.name: spec for spec in _SPECS}


def get_spec(name: str) -> SourceSpec:
    """Return the :class:`SourceSpec` for a source by name.

    Raises:
        KeyError: if no source with that name is registered.
    """
    if name not in SOURCES:
        known = ", ".join(sorted(SOURCES))
        raise KeyError(f"unknown source {name!r}; registered sources: {known}")
    return SOURCES[name]


def all_specs() -> list[SourceSpec]:
    """Return every registered :class:`SourceSpec`."""
    return list(_SPECS)


def dim_sources() -> pd.DataFrame:
    """Render the registry as the ``dim_sources`` catalogue table.

    One row per source, flattening each :class:`SourceSpec` into the columns a
    catalogue consumer needs. This is the third consumer of the single source
    definition (ROADMAP §3.1): the same specs that configure the ingestors and
    supply the DQ thresholds also *are* the published catalogue of what exists.

    Returns:
        pd.DataFrame: one row per source, sorted by group then name.
    """
    rows = [
        {
            "group": spec.group,
            "name": spec.name,
            "n_symbols": len(spec.symbols),
            "symbols": ", ".join(spec.canonical_symbols),
            "intervals": ", ".join(spec.intervals),
            "max_rows_per_call": spec.max_rows_per_call,
            "rate_limit_per_min": spec.rate_limit_per_min,
            "expected_daily_rows": spec.expected_daily_rows,
            "freshness_sla_minutes": spec.freshness_sla_minutes,
            "redistributable": spec.redistributable,
            "license_note": spec.license_note,
        }
        for spec in _SPECS
    ]
    return pd.DataFrame(rows).sort_values(["group", "name"], ignore_index=True)
