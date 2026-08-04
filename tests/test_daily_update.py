"""Daily-update orchestration tests. Offline via the ``offline_binance`` fixture,
which serves canned BTC klines for any known symbol."""

from pathlib import Path

import pandas as pd

from qde.daily_update import run
from qde.storage import upsert_bars


def _seed(base, symbol, source="binance", dates=("2024-01-01", "2024-01-02")):
    idx = pd.DatetimeIndex(pd.to_datetime(list(dates), utc=True), name="date")
    n = len(dates)
    df = pd.DataFrame(
        {"open": [1.0] * n, "high": [2.0] * n, "low": [0.5] * n, "close": [1.5] * n,
         "volume": [10.0] * n},
        index=idx,
    )
    upsert_bars(df, symbol, source, base_dir=base)


def test_run_updates_series_and_writes_summary(tmp_path, offline_binance):
    _seed(str(tmp_path), "BTCUSDT")

    summary = run(str(tmp_path))

    assert summary == {"updated": 1, "failed": 0}
    assert (Path(tmp_path) / "quality_summary.csv").exists()


def test_run_counts_no_new_data_as_updated(tmp_path, monkeypatch):
    # A loader that reports NoNewData means the series is already current, so the
    # run counts it as updated (not failed) and still writes the summary.
    _seed(str(tmp_path), "BTCUSDT")

    import qde.storage as storage
    from qde.loaders import NoNewData

    def no_new(*args, **kwargs):
        raise NoNewData("nothing newer")

    monkeypatch.setattr(storage, "load_ohlcv", no_new)

    summary = run(str(tmp_path))

    assert summary == {"updated": 1, "failed": 0}
    assert (Path(tmp_path) / "quality_summary.csv").exists()


def test_run_counts_loader_error_as_failed(tmp_path, monkeypatch):
    # A real loader failure (an API error, a delisted symbol) is a plain
    # ValueError: it must propagate out of update_ohlcv and be counted as failed,
    # not silently swallowed as "already up to date".
    _seed(str(tmp_path), "BTCUSDT")

    import qde.storage as storage

    def boom(*args, **kwargs):
        raise ValueError("Binance API error 400: Invalid symbol.")

    monkeypatch.setattr(storage, "load_ohlcv", boom)

    summary = run(str(tmp_path))

    assert summary == {"updated": 0, "failed": 1}
    assert (Path(tmp_path) / "quality_summary.csv").exists()


def test_run_skips_a_failing_series(tmp_path, monkeypatch):
    _seed(str(tmp_path), "BTCUSDT")

    # A failing update must be counted and skipped, not abort the run -- and the
    # quality summary should still be rebuilt afterwards.
    import qde.daily_update as du

    def boom(*args, **kwargs):
        raise RuntimeError("update blew up")

    monkeypatch.setattr(du, "update_ohlcv", boom)

    summary = run(str(tmp_path))

    assert summary == {"updated": 0, "failed": 1}
    assert (Path(tmp_path) / "quality_summary.csv").exists()
