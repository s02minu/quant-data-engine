"""Configuration for a streaming capture run."""

from dataclasses import dataclass, field


@dataclass
class StreamConfig:
    """Everything constant about a websocket capture run.

    A deliberately small precursor to the source registry described in
    docs/ROADMAP.md: one place holding identity, subscriptions, and the
    micro-batch knobs, rather than constants scattered through the code.

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
        base_dir: Root of the data lake; bronze output is written under it.
        ws_base_url: Binance websocket base URL.
    """

    source: str = "binance"
    group: str = "microstructure"
    symbols: list[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    kinds: list[str] = field(default_factory=lambda: ["trades", "depth"])
    depth_speed: str = "100ms"
    flush_seconds: int = 30
    base_dir: str = "data"
    ws_base_url: str = "wss://stream.binance.com:9443"

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
            pair = symbol.lower()
            if "trades" in self.kinds:
                names.append(f"{pair}@trade")
            if "depth" in self.kinds:
                names.append(f"{pair}@depth@{self.depth_speed}")
        return names
