"""Configuration for a streaming capture run."""

from typing import Annotated, Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class StreamConfig(BaseSettings):
    """Everything constant about a websocket capture run.

    A pydantic-settings model: the field defaults below are the in-code
    defaults, and every field is overridable from the environment under the
    QDE_ prefix (QDE_SYMBOLS, QDE_FLUSH_SECONDS, QDE_DEPTH_SPEED, ...). Values
    are type-coerced and validated at construction, so a bad override — a
    non-integer QDE_FLUSH_SECONDS or an unsupported QDE_DEPTH_SPEED — fails fast
    at startup rather than deep inside the collector.

    Attributes:
        source: Exchange name. Becomes a partition key on disk.
        group: Shared schema/shape the data belongs to. Becomes a partition key.
        symbols: Canonical uppercase symbols to capture, e.g. ["BTCUSDT"].
        kinds: Message kinds to capture: "trades" (the tick tape) and/or
            "depth" (order-book diffs).
        depth_speed: Binance diff-depth update rate, "100ms" or "1000ms".
        flush_seconds: Seconds to buffer in memory before writing a Parquet
            part file. Larger means fewer, bigger files but more data at risk
            if the process dies before a flush.
        snapshot_seconds: Interval between REST order-book snapshots, which
            anchor the diff-depth stream.
        snapshot_depth: Price levels per snapshot.
        base_dir: Root of the data lake; bronze output is written under it.
        ws_base_url: Binance websocket base URL.
        rest_base_url: Binance REST API base URL, used for snapshots.
    """

    model_config = SettingsConfigDict(env_prefix="QDE_")

    source: str = "binance"
    group: str = "microstructure"
    # NoDecode: take QDE_SYMBOLS / QDE_KINDS as a plain comma-separated string
    # (BTCUSDT,ETHUSDT) rather than pydantic-settings' default JSON decoding of
    # list fields — that default would reject the values docker-compose already
    # passes. The split (and, for symbols, the upper-casing) happens in the
    # validators below.
    symbols: Annotated[list[str], NoDecode] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    kinds: Annotated[list[str], NoDecode] = ["trades", "depth", "book_ticker"]
    depth_speed: Literal["100ms", "1000ms"] = "100ms"
    flush_seconds: int = 30
    snapshot_seconds: int = 300
    snapshot_depth: int = 1000
    base_dir: str = "data"
    ws_base_url: str = "wss://stream.binance.com:9443"
    rest_base_url: str = "https://api.binance.com"

    @field_validator("symbols", "kinds", mode="before")
    @classmethod
    def _split_csv(cls, value: str | list[str]) -> list[str]:
        """Accept a comma-separated string from env, or a list passed from code."""
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("symbols")
    @classmethod
    def _canonical_symbols(cls, symbols: list[str]) -> list[str]:
        """Canonical symbols are uppercase; Binance's lowercasing lives in
        stream_names(), the single boundary that speaks its convention."""
        return [s.upper() for s in symbols]

    def stream_names(self) -> list[str]:
        """Return the Binance stream names to subscribe to.

        Symbols are lowercased here and only here — the single boundary that
        speaks Binance's naming convention, keeping the canonical uppercase
        form everywhere else.

        Returns:
            Flat list such as ["btcusdt@trade", "btcusdt@depth@100ms"].
        """
        names = []
        for symbol in self.symbols:
            pair = symbol.lower()  # the only place Binance's lowercase convention lives
            if "trades" in self.kinds:
                names.append(f"{pair}@trade")
            if "book_ticker" in self.kinds:
                names.append(f"{pair}@bookTicker")  # Binance's camelCase stays at this boundary
            if "depth" in self.kinds:
                names.append(f"{pair}@depth@{self.depth_speed}")
        return names
