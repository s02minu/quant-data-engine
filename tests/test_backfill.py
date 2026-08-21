"""Backfill tests. Offline via the ``offline_binance`` fixture in conftest, which
serves three canned daily BTC klines (2024-01-01..03) for any known symbol."""

from qde.backfill import backfill_bars, backfill_series, backfill_series_group
from qde.registry import get_spec
from qde.storage import _bars_path, _series_path


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


def test_backfill_series_bootstraps_one(tmp_path, offline_fred):
    # A single FRED series is bootstrapped even though nothing is in the lake.
    results = backfill_series_group(
        "2010-01-01", source="fred", series_id="DGS10", base_dir=str(tmp_path)
    )
    assert results == {("fred", "DGS10"): 3}  # the fixture serves three observations
    assert _series_path("DGS10", "fred", str(tmp_path)).exists()


def test_backfill_series_from_registry_seeds_all(tmp_path, offline_fred):
    # --from-registry over the series group seeds every declared FRED series.
    results = backfill_series_group(
        "2010-01-01", source="fred", use_registry=True, base_dir=str(tmp_path)
    )
    assert len(results) == len(get_spec("fred").symbols)  # the full curated spine
    assert all(src == "fred" for (src, _sid) in results)


def test_a_failing_series_is_recorded_not_just_skipped(tmp_path, monkeypatch):
    # A failure is skipped so one bad symbol cannot discard a long backfill's real
    # work — but skipping it *and* forgetting it made a run where every series
    # failed look identical to a clean one, exit code included.
    import qde.backfill as backfill_mod

    def boom(*_a, **_k):
        raise ValueError("upstream refused")

    monkeypatch.setattr(backfill_mod, "backfill_series", boom)

    failures: list[str] = []
    results = backfill_bars(
        "2024-01-01", source="binance", symbol="BTCUSDT",
        base_dir=str(tmp_path), use_registry=True, failures=failures,
    )

    assert results == {}
    assert len(failures) == 1
    assert "binance/BTCUSDT" in failures[0] and "ValueError" in failures[0]


def test_callers_that_do_not_ask_for_failures_still_work(tmp_path, monkeypatch):
    # The parameter is optional precisely so every existing caller is untouched.
    import qde.backfill as backfill_mod

    def boom(*_a, **_k):
        raise ValueError("upstream refused")

    monkeypatch.setattr(backfill_mod, "backfill_series", boom)
    assert backfill_bars(
        "2024-01-01", source="binance", symbol="BTCUSDT",
        base_dir=str(tmp_path), use_registry=True,
    ) == {}


def test_credentials_are_loaded_for_every_group(monkeypatch, tmp_path):
    """The load used to sit inside the series/events branches only.

    That held while FRED was the only source needing a key, and broke the moment
    Tiingo became the first `bars` source with credentials: all 27 symbols failed
    for want of a token the process had never loaded. Which group needs a secret is
    not the entry point's judgment to make.
    """
    import sys

    import qde.backfill as backfill

    called: list[str] = []
    monkeypatch.setattr(backfill, "load_secrets", lambda *a, **k: called.append("loaded"))
    monkeypatch.setattr(backfill, "backfill_bars", lambda **kw: {"series": 0, "rows": 0})
    monkeypatch.setattr(
        sys, "argv",
        ["qde.backfill", "--group", "bars", "--from", "2024-01-01",
         "--base-dir", str(tmp_path)],
    )

    backfill.main()
    assert called == ["loaded"], "a bars backfill must load credentials too"
