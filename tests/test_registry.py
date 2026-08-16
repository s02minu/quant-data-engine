"""Tests for the source registry (the little book)."""

import pandas as pd
import pytest

from qde.registry import (
    SOURCES,
    SourceSpec,
    all_specs,
    declared_series,
    dim_sources,
    get_spec,
)


def test_every_source_writes_a_known_group():
    """Groups are the four shared shapes; a typo here misroutes partitions."""
    valid_groups = {"bars", "series", "events", "microstructure"}
    for spec in all_specs():
        assert spec.group in valid_groups


def test_a_source_is_identified_by_group_and_name():
    """A venue can appear in more than one group — Binance is bars *and*
    microstructure — so identity is the pair, not the name alone."""
    keys = [(spec.group, spec.name) for spec in all_specs()]
    assert len(keys) == len(set(keys))


def test_the_ingestor_index_is_unambiguous():
    """``SOURCES`` resolves a name to the ingestor a batch job should use, so its
    names must stay unique — a duplicate would silently replace one definition with
    another and every backfill for it would quietly use the wrong spec."""
    batch = [s for s in all_specs() if s.group != "microstructure"]
    assert sorted(SOURCES) == sorted(s.name for s in batch)
    assert len(SOURCES) == len(batch)


def test_a_duplicate_name_is_rejected_rather_than_absorbed():
    from qde.registry.sources import _index_by_name

    spec = SOURCES["binance"]
    with pytest.raises(ValueError, match="duplicate source name"):
        _index_by_name([spec, spec])


def test_get_spec_roundtrips_and_rejects_unknown():
    assert get_spec("binance").name == "binance"
    with pytest.raises(KeyError, match="unknown source"):
        get_spec("nope")


def test_native_translates_canonical_to_source_spelling():
    """The symbol map folded in from the old SYMBOL_MAP still translates."""
    assert get_spec("kraken").native("BTCUSDT") == "XBTUSD"
    assert get_spec("binance").native("BTCUSDT") == "BTCUSDT"  # identity
    with pytest.raises(KeyError, match="not a symbol"):
        get_spec("kraken").native("SOLUSDT")  # kraken does not carry SOL here


def test_canonical_symbols_are_the_map_keys():
    spec = get_spec("yfinance")
    assert spec.canonical_symbols == list(spec.symbols)
    assert "DX-Y.NYB" in spec.canonical_symbols


def test_empty_symbols_is_rejected():
    with pytest.raises(ValueError, match="at least one symbol"):
        SourceSpec(group="bars", name="broken", symbols={})


def test_yfinance_is_marked_non_redistributable():
    """The licensing audit (Phase 2) lives on the spec; yfinance is code-only."""
    spec = get_spec("yfinance")
    assert spec.redistributable is False
    assert spec.license_note  # a reason must be recorded
    # Exchange-native crypto is redistributable.
    assert get_spec("binance").redistributable is True


def test_declared_series_enumerates_every_symbol_interval():
    all_series = declared_series()
    # One tuple per (source, symbol, interval); matches the folded-in SYMBOL_MAP.
    # Batch groups only: a streamed venue has no ingestor and nothing to backfill,
    # so enumerating it here would report permanent drift that can never be closed.
    expected = {
        (spec.name, symbol, interval)
        for spec in all_specs()
        if spec.group != "microstructure"
        for symbol in spec.canonical_symbols
        for interval in spec.intervals
    }
    assert set(all_series) == expected
    # Tuples are (source, symbol, interval) — same order as list_bars_series.
    assert ("kraken", "ETHUSDT", "1d") in all_series
    assert ("binance", "SOLUSDT", "1d") in all_series


def test_declared_series_filters_by_group():
    bars = declared_series(group="bars")
    series = declared_series(group="series")
    events = declared_series(group="events")
    # The groups partition the full declared set.
    assert set(bars) | set(series) | set(events) == set(declared_series())
    assert set(bars).isdisjoint(series)
    assert set(events).isdisjoint(bars) and set(events).isdisjoint(series)
    # FRED is a `series` source, not a bars source.
    assert all(src != "fred" for (src, _sym, _iv) in bars)
    assert ("fred", "CPIAUCSL", "1d") in series  # interval is a bars-ism, unused for series
    # fredcal is the events (calendar) source, distinct from the fred series source.
    assert ("fredcal", "CPIAUCSL", "1d") in events
    assert declared_series(group="microstructure") == []  # none registered here yet


def test_dim_sources_has_one_row_per_source():
    df = dim_sources()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(all_specs())
    # Streamed venues reuse a batch venue's name, so the catalogue carries more rows
    # than the ingestor index has entries — by group+name it is still one each.
    assert set(df["name"]) >= set(SOURCES)
    assert len(set(zip(df["group"], df["name"], strict=True))) == len(df)
    # The catalogue carries the licensing decision for the publishing gate.
    assert {"redistributable", "license_note", "group"} <= set(df.columns)
