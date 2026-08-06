"""Ingestors — the ``BaseIngestor`` and its per-source implementations.

Each concrete ingestor is bound to a :class:`~qde.registry.spec.SourceSpec` and
implements the little book's fetch/normalize contract. ``get_ingestor`` resolves
a source name to a ready-to-use ingestor via the registry.
"""

from qde.ingest.base import BaseIngestor, RawPage
from qde.ingest.binance import BinanceIngestor
from qde.ingest.cboe import CboeIngestor
from qde.ingest.cftc import CftcIngestor
from qde.ingest.fred import FredIngestor
from qde.ingest.kraken import KrakenIngestor
from qde.ingest.yfinance import YfinanceIngestor
from qde.registry import get_spec

__all__ = ["BaseIngestor", "RawPage", "get_ingestor"]

# Source name -> ingestor class. A registered source without an ingestor here is
# declared (it appears in the catalogue) but not yet fetchable.
_INGESTORS: dict[str, type[BaseIngestor]] = {
    "binance": BinanceIngestor,
    "kraken": KrakenIngestor,
    "yfinance": YfinanceIngestor,
    "fred": FredIngestor,
    "cboe": CboeIngestor,
    "cftc": CftcIngestor,
}


def get_ingestor(source: str) -> BaseIngestor:
    """Return a ready-to-use ingestor for a source name.

    Resolves the source's :class:`SourceSpec` from the registry and constructs
    its ingestor with it.

    Raises:
        KeyError: if the source is unregistered, or is registered but has no
            ingestor implementation yet.
    """
    spec = get_spec(source)  # raises KeyError on an unknown source
    cls = _INGESTORS.get(source)
    if cls is None:
        raise KeyError(f"source {source!r} is registered but has no ingestor")
    return cls(spec)
