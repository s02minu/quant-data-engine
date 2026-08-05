"""Source registry — the little book.

Each source is declared once as a :class:`SourceSpec` (see ``spec``) and
collected in the registry (see ``sources``). One definition feeds ingestor
config, the data-quality contract, and the public catalogue.
"""

from qde.registry.sources import (
    SOURCES,
    all_specs,
    declared_series,
    dim_sources,
    get_spec,
)
from qde.registry.spec import SourceSpec

__all__ = [
    "SOURCES",
    "SourceSpec",
    "all_specs",
    "declared_series",
    "dim_sources",
    "get_spec",
]
