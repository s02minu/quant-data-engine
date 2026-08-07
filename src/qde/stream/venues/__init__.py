"""Venue adapters: one per exchange dialect, resolved by name.

`get_adapter(config.source)` hands the collector the right adapter, mirroring
how `qde.ingest.get_ingestor` resolves a batch source from the registry.
"""

from qde.stream.venues.base import VenueAdapter
from qde.stream.venues.binance import BinanceAdapter

_ADAPTERS: dict[str, type[VenueAdapter]] = {
    BinanceAdapter.name: BinanceAdapter,
}


def get_adapter(source: str) -> VenueAdapter:
    """Return the adapter for a source name (the partition `source=` key).

    Raises:
        ValueError: If no adapter is registered for the source.
    """
    try:
        return _ADAPTERS[source]()
    except KeyError:
        known = ", ".join(sorted(_ADAPTERS))
        raise ValueError(f"No stream adapter for source {source!r}; known: {known}") from None


__all__ = ["VenueAdapter", "get_adapter"]
