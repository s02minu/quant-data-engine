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

# Curated FRED macro spine for the platform's `series` group. Deliberately
# limited to U.S.-government statistical series (BLS / BEA / Census / Federal
# Reserve Board / Treasury), which are public domain and so redistributable.
# Third-party series FRED also hosts (ICE credit indices, S&P/Case-Shiller) are
# intentionally excluded because they are not redistributable (see the spec's
# license_note and docs/data-sources.md §4).
_FRED_SERIES: list[str] = [
    # growth / activity / consumer
    "GDPC1",  # real GDP
    "INDPRO",  # industrial production
    "HOUST",  # housing starts
    "RSAFS",  # advance retail sales (consumer demand)
    # inflation
    "CPIAUCSL",  # CPI, all items
    "CPILFESL",  # core CPI
    "PCEPI",  # PCE price index
    "PCEPILFE",  # core PCE
    # labour
    "UNRATE",  # unemployment rate
    "PAYEMS",  # nonfarm payrolls
    "ICSA",  # initial jobless claims
    # rates / curve / funding
    "FEDFUNDS",  # effective fed funds rate
    "SOFR",  # secured overnight financing rate (modern funding benchmark)
    "DGS3MO",  # 3m Treasury (front of the curve)
    "DGS2",  # 2y Treasury
    "DGS10",  # 10y Treasury
    "DGS30",  # 30y Treasury
    "T10Y2Y",  # 10y-2y spread
    "T10Y3M",  # 10y-3m spread (classic recession signal)
    "DFII10",  # 10y TIPS (real yield)
    "T10YIE",  # 10y breakeven inflation
    # money / liquidity plumbing
    "M2SL",  # M2 money stock
    "WALCL",  # Fed balance sheet (total assets)
    "RRPONTSYD",  # overnight reverse repo
    "WTREGEN",  # Treasury General Account
    # dollar
    "DTWEXBGS",  # trade-weighted USD (broad)
]

# The CBOE volatility complex for the `series` group: end-of-day levels for the
# VIX (equity implied vol), VVIX (vol-of-vol) and SKEW (tail-risk) indices —
# Model-1 volatility inputs (docs/data-sources.md §3.1). Served as public CSVs on
# the CBOE CDN; EOD index *levels* are redistributable (the real-time feed and the
# underlying options data are not). Each is `{SYMBOL}_History.csv`.
_CBOE_SERIES: list[str] = ["VIX", "VVIX", "SKEW"]

# Curated CFTC COT positioning markets (`series`, multi-metric): a friendly ticker
# -> the CFTC contract market code the Socrata TFF dataset keys on. The strategy's
# positioning inputs (docs/data-sources.md §2/§3.1): equity index, the Treasury
# curve + funding, the dollar and FX majors, VIX, and the two CME crypto markets.
# Codes verified against the live TFF futures-only universe (gpe5-46if). `FF` and
# `VIX` are deliberately distinct series_ids from the FRED/CBOE ones — they are
# futures *positioning*, source-scoped, not the rate/level of the same name.
_CFTC_MARKETS: dict[str, str] = {
    # equity index
    "ES": "13874A",  # E-MINI S&P 500
    "NQ": "209742",  # NASDAQ MINI
    "RTY": "239742",  # RUSSELL E-MINI
    # rates / curve / funding
    "UST2Y": "042601",  # UST 2Y NOTE
    "UST5Y": "044601",  # UST 5Y NOTE
    "UST10Y": "043602",  # UST 10Y NOTE
    "USTBOND": "020601",  # UST BOND (the long bond)
    "FF": "045601",  # FED FUNDS
    # dollar + FX majors
    "DXY": "098662",  # ICE USD INDEX
    "EUR": "099741",  # EURO FX
    "JPY": "097741",  # JAPANESE YEN
    "GBP": "096742",  # BRITISH POUND
    "CHF": "092741",  # SWISS FRANC
    "AUD": "232741",  # AUSTRALIAN DOLLAR
    "CAD": "090741",  # CANADIAN DOLLAR
    # volatility
    "VIX": "1170E1",  # VIX FUTURES
    # crypto (CME)
    "BTC": "133741",  # BITCOIN
    "ETH": "146021",  # ETHER CASH SETTLED
}

# ccxt-backed bars exchanges (Wave 2 coverage). ccxt uses *unified* symbols, so the
# canonical->native map is the same shape for every venue — only the quote asset
# differs: USDT-quoted venues vs Coinbase, which lists the USD pair. (BTC/USD and
# BTC/USDT are near-identical for bar coverage; the small USD-vs-stablecoin nuance
# is accepted so one canonical symbol spans venues.)
_CCXT_USDT = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}
_CCXT_USD = {"BTCUSDT": "BTC/USD", "ETHUSDT": "ETH/USD", "SOLUSDT": "SOL/USD"}


def _ccxt_spec(name: str, symbols: dict[str, str]) -> "SourceSpec":
    """A bars SourceSpec for a ccxt exchange (name == ccxt exchange id)."""
    return SourceSpec(
        group="bars",
        name=name,
        symbols=symbols,
        intervals=["1d"],
        # A hint only: ccxt clamps it to each venue's own cap, and the ingestor
        # paginates by advancing time, not by assuming a full page.
        max_rows_per_call=1000,
        rate_limit_per_min=None,  # ccxt's enableRateLimit paces requests per venue
        expected_daily_rows=1,
        null_tolerance=_OHLCV_NO_NULLS,
        redistributable=True,
        license_note=(
            f"{name} public spot OHLCV via ccxt; exchange-native market data, "
            "generally redistributable. Verify the venue's API terms before "
            "public publishing."
        ),
    )


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
    # ccxt-backed spot exchanges (Wave 2): new venues via one shared ingestor.
    # Kraken/Binance stay on their bespoke, byte-for-byte-validated ingestors.
    _ccxt_spec("coinbase", _CCXT_USD),
    _ccxt_spec("bybit", _CCXT_USDT),
    _ccxt_spec("okx", _CCXT_USDT),
    _ccxt_spec("kucoin", _CCXT_USDT),
    SourceSpec(
        group="series",
        name="fred",
        # FRED series ids are already the project-canonical identifier — identity map.
        symbols={sid: sid for sid in _FRED_SERIES},
        # `intervals` is unused for `series`: frequency is a per-series property of
        # the source, not a pull parameter. Left at the default.
        max_rows_per_call=100000,  # the observations endpoint's page cap
        rate_limit_per_min=120,  # FRED's documented request budget
        # A missing observation (FRED's ".") is a legitimate "not published"
        # marker, not a defect — so `value` nulls are fully tolerated; the DQ
        # check that matters is a missing *row* where the release calendar
        # expected one (see docs/schemas/series.md).
        null_tolerance={"value": 1.0},
        redistributable=True,
        license_note=(
            "FRED delivery of U.S.-government statistical series (BLS / BEA / "
            "Census / Federal Reserve Board / Treasury) — public domain and "
            "redistributable. NOTE: FRED also hosts third-party series (ICE, "
            "S&P/Case-Shiller) that are NOT redistributable; only government "
            "series are curated here. Re-verify per series before public publishing."
        ),
    ),
    SourceSpec(
        group="series",
        name="cboe",
        # CBOE index tickers are already the project-canonical identifier — identity map.
        symbols={sid: sid for sid in _CBOE_SERIES},
        # `intervals` is unused for `series` (frequency is a per-series property,
        # not a pull parameter). Left at the default.
        max_rows_per_call=None,  # the CDN serves the whole history in one CSV
        rate_limit_per_min=None,  # static CDN files; no documented request budget
        # An index level is present on every trading day, so a missing `value`
        # would be a real defect (a parse failure or a dropped print), not a
        # tolerated gap the way FRED's not-yet-published "." is — tolerate none.
        null_tolerance={"value": 0.0},
        redistributable=True,
        license_note=(
            "CBOE end-of-day volatility index levels (VIX / VVIX / SKEW), "
            "published as free CSVs on the CBOE CDN. EOD index levels are "
            "generally redistributable; the real-time feed and the underlying "
            "options data are NOT. Re-verify CBOE's terms before public publishing."
        ),
    ),
    SourceSpec(
        group="series",
        name="cftc",
        # Friendly ticker -> CFTC contract market code (a real canonical->native map,
        # unlike FRED/CBOE's identity maps): the lake stores series_id=ES, the API
        # is queried by code 13874A.
        symbols=_CFTC_MARKETS,
        max_rows_per_call=50000,  # Socrata's per-request row cap
        rate_limit_per_min=None,  # anonymous Socrata throttle; weekly volume is tiny
        # COT is weekly (Tue report, Fri release), not daily. The `series` machinery
        # is frequency-agnostic; there is no `frequency` field on the spec yet, so
        # `expected_daily_rows` stays at its default and the weekly cadence is a DQ
        # concern for Phase 9 (freshness), not an ingestion parameter.
        # Positioning counts are stored per metric under the `value` column; a null
        # is a "not reported" marker (a thin/!new market), tolerated like FRED's.
        null_tolerance={"value": 1.0},
        redistributable=True,
        license_note=(
            "CFTC Commitments of Traders — Traders in Financial Futures (TFF), "
            "futures-only — from the CFTC public reporting Socrata API. "
            "U.S.-government public-domain data; redistributable. Stored as "
            "multi-metric positioning (trader-category long/short + open interest)."
        ),
    ),
    SourceSpec(
        group="series",
        name="binancefut",
        # Binance USD-M perps use the project-canonical spelling — identity map.
        # This is the derivatives feed, distinct from the spot `binance` bars source.
        symbols={"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT", "SOLUSDT": "SOLUSDT"},
        max_rows_per_call=1000,  # the fundingRate endpoint's page cap
        rate_limit_per_min=1200,  # fapi request-weight budget; a funding call is cheap
        # Funding settles every 8h -> 3 rows/day per metric (a DQ hint; not enforced
        # until Phase 9). mark_price is NaN for the earliest settlements, so tolerate
        # value nulls like FRED rather than flag them as a defect.
        expected_daily_rows=3,
        null_tolerance={"value": 1.0},
        redistributable=True,
        license_note=(
            "Binance USD-M perpetual funding rate + settlement mark price "
            "(public fapi REST); exchange-native and generally redistributable. "
            "Open interest (~30d REST history) and liquidations (no public REST, "
            "streaming only) are deliberately not included here — see the ingestor. "
            "Verify Binance API terms before public publishing."
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


def declared_series(group: str | None = None) -> list[tuple[str, str, str]]:
    """Return every ``(source, symbol, interval)`` the registry declares.

    This is the *intended* full set of series — every symbol × interval across
    every spec — independent of what has actually been seeded into the lake.
    Diff it against the seeded set (``qde.storage.list_bars_series``) to surface
    drift (declared but not yet backfilled), or enumerate it to drive a
    registry-based backfill that can seed new series.

    Args:
        group: If given, restrict to sources writing that group (e.g. ``"bars"``);
            otherwise include every group.

    Returns:
        List of ``(source, symbol, interval)`` tuples, matching the column order
        of ``list_bars_series`` so the two sets can be diffed directly.
    """
    return [
        (spec.name, symbol, interval)
        for spec in _SPECS
        if group is None or spec.group == group
        for symbol in spec.canonical_symbols
        for interval in spec.intervals
    ]


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
