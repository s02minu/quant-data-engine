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
        {
            "open": [1.0] * n,
            "high": [2.0] * n,
            "low": [0.5] * n,
            "close": [1.5] * n,
            "volume": [10.0] * n,
        },
        index=idx,
    )
    upsert_bars(df, symbol, source, base_dir=base)


def test_run_updates_series_and_writes_summary(tmp_path, offline_binance):
    _seed(str(tmp_path), "BTCUSDT")

    summary = run(str(tmp_path))

    assert summary["updated"] == 1 and summary["failed"] == 0
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

    assert summary["updated"] == 1 and summary["failed"] == 0
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

    assert summary["updated"] == 0 and summary["failed"] == 1
    # The failure detail (not just a count) is captured for alerting.
    assert summary["failures"][0]["label"] == "binance/BTCUSDT/1d"
    assert "Invalid symbol" in summary["failures"][0]["detail"]
    assert (Path(tmp_path) / "quality_summary.csv").exists()


def test_run_updates_a_seeded_series(tmp_path, offline_fred):
    # A seeded FRED series is discovered and incrementally updated alongside bars.
    from qde.storage import upsert_series

    idx = pd.DatetimeIndex(pd.to_datetime(["2023-12-01"], utc=True), name="date")
    upsert_series(
        pd.DataFrame({"value": [1.0]}, index=idx), "DGS10", "fred", base_dir=str(tmp_path)
    )

    summary = run(str(tmp_path))

    assert summary["updated"] == 1 and summary["failed"] == 0
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

    assert summary["updated"] == 0 and summary["failed"] == 1
    assert (Path(tmp_path) / "quality_summary.csv").exists()


# --- intake verification must reach all three groups, not just bars ---------------
#
# `verify_frame` had exactly one production call site (bars). The series and events
# contracts were written, tested, and never applied to anything the running system
# fetched — two thirds of the contract was dead code in production.


def test_a_defective_series_frame_is_verified_at_intake(tmp_path, monkeypatch, offline_fred):
    from qde.storage import upsert_series

    idx = pd.DatetimeIndex(pd.to_datetime(["2023-12-01"], utc=True), name="date")
    upsert_series(
        pd.DataFrame({"value": [1.0]}, index=idx), "DGS10", "fred", base_dir=str(tmp_path)
    )

    # An ingestor that forgot to coerce FRED's "." missing marker. Every row is
    # present and the frame looks entirely normal.
    unparsed = pd.DataFrame(
        {"value": ["."] * 3},
        index=pd.DatetimeIndex(
            pd.to_datetime(["2023-12-02", "2023-12-03", "2023-12-04"], utc=True), name="date"
        ),
    )

    class _Ingestor:
        def load(self, *a, **k):
            return unparsed

    monkeypatch.setattr("qde.ingest.get_ingestor", lambda source: _Ingestor())

    summary = run(str(tmp_path))
    assert any(v.check == "numeric" and v.group == "series" for v in summary["violations"]), (
        "an unparsed series frame must be caught as it arrives"
    )


def test_the_boundary_row_a_source_re_serves_is_not_a_range_violation(tmp_path, monkeypatch):
    # An incremental pull asks for watermark+1, but FRED answers a monthly series with
    # the period-boundary observation it already served. That is normal and the upsert
    # is idempotent — yet graded against `next_day` it reads as "the range parameters
    # were ignored". Ten fired on the first live run of this wiring.
    from qde.storage import upsert_series

    idx = pd.DatetimeIndex(pd.to_datetime(["2026-07-01"], utc=True), name="date")
    upsert_series(pd.DataFrame({"value": [1.0]}, index=idx), "PAYEMS", "fred", str(tmp_path))

    resent = pd.DataFrame(
        {"value": [1.0, 2.0]},
        index=pd.DatetimeIndex(
            pd.to_datetime(["2026-07-01", "2026-08-01"], utc=True), name="date"
        ),
    )

    class _Ingestor:
        def load(self, *a, **k):
            return resent

    monkeypatch.setattr("qde.ingest.get_ingestor", lambda source: _Ingestor())
    summary = run(str(tmp_path))
    assert not [v for v in summary["violations"] if v.check == "range"], (
        "re-serving the watermark row must not read as an ignored date range"
    )
