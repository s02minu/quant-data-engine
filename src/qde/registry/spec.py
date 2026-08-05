"""The ``SourceSpec`` — one entry in the source registry (the little book).

A ``SourceSpec`` holds everything *constant* about a data source in a single
declarative definition, so that the same knowledge is not written three times.
One spec feeds three consumers (ROADMAP §3.1):

1. **Config** for the ingestor — endpoints, page size, rate limit, symbols.
2. **A data-quality contract** — expected row counts, null tolerances, freshness
   SLA — read by the quality checks instead of being hardcoded.
3. **A row in the published catalogue** (``dim_sources``) — including whether the
   data may be redistributed (§6).

Because the spec is the single source of truth, the DQ thresholds cannot drift
away from the config, and the catalogue cannot drift away from either.
"""

from pydantic import BaseModel, Field, field_validator


class SourceSpec(BaseModel):
    """Everything constant about one data source.

    Attributes:
        group: The shared schema/shape the source writes to — one of ``bars``,
            ``series``, ``events``, ``microstructure`` (ROADMAP §3.3). Becomes
            the outermost partition key on disk. Grouping is by *shape*, not
            asset class, so a new instrument type is a new registry row, not a
            new module.
        name: The source's short name, e.g. ``"binance"``. Unique across the
            registry and used as the ``source`` partition key.
        symbols: Canonical-to-native symbol map. Keys are the project's
            canonical uppercase symbols (``"BTCUSDT"``); values are how *this*
            source spells them (Kraken's ``"XBTUSD"``). This folds the old
            standalone ``SYMBOL_MAP`` into the registry, so a source's symbol
            translation lives with the rest of its definition.
        intervals: Bar sizes the source is configured to serve, e.g. ``["1d"]``.
        max_rows_per_call: Page size for a paginated pull, or ``None`` for a
            source that returns a whole range in one request (yfinance).
        rate_limit_per_min: Requests allowed per minute, or ``None`` if the
            source is not meaningfully rate-limited for this usage.
        expected_daily_rows: Rows a healthy series gains per active day, per
            symbol — the anomaly threshold the quality checks compare against.
            One for a daily bar; a market-closed day legitimately adds zero.
        null_tolerance: Maximum acceptable null fraction per column, e.g.
            ``{"close": 0.0}``. A column exceeding its tolerance fails the DQ
            contract.
        freshness_sla_minutes: How stale the newest bar may be before the series
            is considered late. Daily bars settle once a day, so the SLA is
            generous (a bit over 24h).
        redistributable: Whether the platform may republish this data in the
            public lake (§6). ``False`` sources are code-only: the ingestor is
            open-sourced, but the data is not published. The publishing job
            refuses to write a ``False`` source into the public bucket.
        license_note: Human-readable reason behind ``redistributable`` — the
            licence or terms that decide it. Surfaced in the catalogue.
    """

    group: str
    name: str
    symbols: dict[str, str]
    intervals: list[str] = Field(default_factory=lambda: ["1d"])
    max_rows_per_call: int | None = None
    rate_limit_per_min: int | None = None
    expected_daily_rows: int = 1
    null_tolerance: dict[str, float] = Field(default_factory=dict)
    freshness_sla_minutes: int = 24 * 60 + 60  # a daily bar, plus an hour of slack
    redistributable: bool = True
    license_note: str = ""

    @field_validator("symbols")
    @classmethod
    def _non_empty_symbols(cls, symbols: dict[str, str]) -> dict[str, str]:
        """A source with no symbols has nothing to ingest — reject it early."""
        if not symbols:
            raise ValueError("a SourceSpec must declare at least one symbol")
        return symbols

    @property
    def canonical_symbols(self) -> list[str]:
        """The project-canonical symbols this source provides."""
        return list(self.symbols)

    def native(self, symbol: str) -> str:
        """Translate a canonical symbol to this source's native spelling.

        Args:
            symbol: A canonical uppercase symbol, e.g. ``"BTCUSDT"``.

        Returns:
            The source-native symbol, e.g. Kraken's ``"XBTUSD"``.

        Raises:
            KeyError: if the symbol is not one this source provides.
        """
        if symbol not in self.symbols:
            raise KeyError(f"{symbol!r} is not a symbol of source {self.name!r}")
        return self.symbols[symbol]
