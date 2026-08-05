"""Backfill tests. Offline via the ``offline_binance`` fixture in conftest, which
serves three canned daily BTC klines (2024-01-01..03) for any known symbol."""

from qde.backfill import backfill_bars, backfill_series
from qde.storage import _bars_path


def test_backfill_bootstraps_new_series(tmp_path, offline_binance):
    # source + symbol names a series directly; it need not already exist.
    results = backfill_bars(
        start="2024-01-01", source="binance", symbol="BTCUSDT", base_dir=str(tmp_path)
    )

    assert results == {("BTCUSDT", "binance", "1d"): 3}
    assert _bars_path("BTCUSDT", "binance", "1d", str(tmp_path)).exists()


def test_backfill_is_idempotent(tmp_path, offline_binance):
    first = backfill_bars("2024-01-01", source="binance", symbol="BTCUSDT", base_dir=str(tmp_path))
    second = backfill_bars("2024-01-01", source="binance", symbol="BTCUSDT", base_dir=str(tmp_path))

    assert first == second == {("BTCUSDT", "binance", "1d"): 3}


def test_backfill_refreshes_all_existing_series(tmp_path, offline_binance):
    # Bootstrap two series...
    backfill_series("BTCUSDT", "binance", "1d", "2024-01-01", base_dir=str(tmp_path))
    backfill_series("ETHUSDT", "binance", "1d", "2024-01-01", base_dir=str(tmp_path))

    # ...then a filter-less backfill discovers and refreshes both from the lake.
    results = backfill_bars("2024-01-01", base_dir=str(tmp_path))

    assert set(results) == {("BTCUSDT", "binance", "1d"), ("ETHUSDT", "binance", "1d")}


def test_backfill_skips_failing_series(tmp_path, offline_binance):
    # An unmapped symbol raises in the loader; the run must skip it, not abort.
    results = backfill_bars(
        "2024-01-01", source="binance", symbol="NOTAREALTICKER", base_dir=str(tmp_path)
    )

    assert results == {}


def test_backfill_from_registry_seeds_declared_series(tmp_path, offline_binance):
    # --from-registry enumerates the registry's declared set, so an unseeded
    # series is included and bootstrapped: all three binance symbols land even
    # though nothing was in the lake to begin with (SOLUSDT was never seeded).
    results = backfill_bars(
        "2024-01-01", source="binance", use_registry=True, base_dir=str(tmp_path)
    )

    assert set(results) == {
        ("BTCUSDT", "binance", "1d"),
        ("ETHUSDT", "binance", "1d"),
        ("SOLUSDT", "binance", "1d"),
    }
