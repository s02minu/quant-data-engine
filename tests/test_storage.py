from pathlib import Path

import pandas as pd
import pytest

import qde.storage as storage
from qde.loaders import NoNewData
from qde.storage import (
    _bars_path,
    bars_watermark,
    list_bars_series,
    load_ohlcv_local,
    query,
    save_ohlcv,
    update_ohlcv,
    upsert_bars,
)


def _sample_bars(dates, close=1.5):
    """Build a minimal bars frame indexed by a UTC ``date`` index."""
    idx = pd.DatetimeIndex(pd.to_datetime(dates, utc=True), name="date")
    n = len(dates)
    return pd.DataFrame(
        {"open": [1.0] * n, "high": [2.0] * n, "low": [0.5] * n, "close": [close] * n,
         "volume": [10.0] * n},
        index=idx,
    )


# Assert the saved file exists on disk. offline_binance mocks the klines fetch so
# save_ohlcv exercises the fetch->save path without touching the live API — these
# tests otherwise reached api.binance.com and only passed from a non-US IP.
def test_save_create_file(offline_binance, tmp_path):
    path = save_ohlcv(
        symbol="BTCUSDT",
        source="binance",
        start="2024-01-01",
        end="2024-01-05",
        base_dir=str(tmp_path),
    )
    assert Path(path).exists()


def test_load_reads_saved_file(offline_binance, tmp_path):
    # Save first
    save_ohlcv(
        "BTCUSDT", source="binance", start="2024-01-01", end="2024-01-05", base_dir=str(tmp_path)
    )

    # Load it back
    df = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))

    assert not df.empty
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]


def test_update_add_new_data(tmp_path, monkeypatch):
    # Seed two days, then have the loader return later days; the watermark-driven
    # update should fetch past the last stored date and grow the series. The loader
    # is mocked (like the other update tests) so this never hits the live API.
    upsert_bars(
        _sample_bars(["2024-01-01", "2024-01-02"]), "BTCUSDT", "binance", base_dir=str(tmp_path)
    )
    df_before = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))

    def newer(*args, **kwargs):
        return _sample_bars(["2024-01-03", "2024-01-04"])

    monkeypatch.setattr(storage, "load_ohlcv", newer)
    update_ohlcv("BTCUSDT", source="binance", base_dir=str(tmp_path))

    df_after = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))
    assert len(df_after) > len(df_before)


def test_update_no_new_data_is_up_to_date(tmp_path, monkeypatch):
    # A NoNewData from the loader means the source has nothing newer: the update
    # returns quietly and leaves the stored series untouched.
    upsert_bars(
        _sample_bars(["2024-01-01", "2024-01-02"]), "BTCUSDT", "binance", base_dir=str(tmp_path)
    )

    def no_new(*args, **kwargs):
        raise NoNewData("nothing newer")

    monkeypatch.setattr(storage, "load_ohlcv", no_new)

    update_ohlcv("BTCUSDT", source="binance", base_dir=str(tmp_path))

    out = load_ohlcv_local("BTCUSDT", "binance", base_dir=str(tmp_path))
    assert len(out) == 2


def test_update_skips_fetch_when_watermark_is_already_today(tmp_path, monkeypatch):
    # Seed a bar dated "today" (UTC) -- e.g. a venue that already publishes a
    # same-day preliminary candle. The next incremental start would be tomorrow,
    # which cannot exist yet: the update must skip the fetch entirely rather than
    # call the loader, since some venues (Coinbase, observed live) hard-reject a
    # future start instead of returning empty like the rest.
    today = pd.Timestamp.now(tz="UTC").normalize()
    # allow_forming: this test deliberately reproduces a venue publishing a same-day
    # preliminary candle, which upsert_bars now refuses by default.
    upsert_bars(
        _sample_bars([str(today.date())]), "BTCUSDT", "coinbase",
        base_dir=str(tmp_path), allow_forming=True,
    )

    def boom(*args, **kwargs):
        raise AssertionError("loader must not be called when the watermark is already today")

    monkeypatch.setattr(storage, "load_ohlcv", boom)

    update_ohlcv("BTCUSDT", source="coinbase", base_dir=str(tmp_path))  # must not raise

    out = load_ohlcv_local("BTCUSDT", "coinbase", base_dir=str(tmp_path))
    assert len(out) == 1  # untouched


def test_update_propagates_real_loader_error(tmp_path, monkeypatch):
    # A real failure (an API error, a delisted symbol) is a plain ValueError and
    # must propagate -- not be swallowed as "already up to date".
    upsert_bars(
        _sample_bars(["2024-01-01", "2024-01-02"]), "BTCUSDT", "binance", base_dir=str(tmp_path)
    )

    def boom(*args, **kwargs):
        raise ValueError("Binance API error 500: internal error")

    monkeypatch.setattr(storage, "load_ohlcv", boom)

    with pytest.raises(ValueError, match="Binance API error 500"):
        update_ohlcv("BTCUSDT", source="binance", base_dir=str(tmp_path))


def test_query_reads_bars_lake(tmp_path):
    # Write a tiny bars file straight into the partitioned bronze layout.
    df = pd.DataFrame(
        {"open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [10.0]},
        index=pd.DatetimeIndex(["2024-01-01"], tz="UTC", name="date"),
    )
    path = _bars_path("BTCUSDT", "binance", "1d", base_dir=str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")

    # The hive partition keys (source/symbol/interval) come back as columns.
    out = query("SELECT symbol, source, interval, close FROM bars", base_dir=str(tmp_path))

    assert out.loc[0, "symbol"] == "BTCUSDT"
    assert out.loc[0, "source"] == "binance"
    assert out.loc[0, "interval"] == "1d"
    assert out.loc[0, "close"] == 1.5


def _write_bar(tmp_path, symbol, source="binance", interval="1d"):
    df = pd.DataFrame(
        {"open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [10.0]},
        index=pd.DatetimeIndex(["2024-01-01"], tz="UTC", name="date"),
    )
    path = _bars_path(symbol, source, interval, base_dir=str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")


def test_list_bars_series_empty(tmp_path):
    out = list_bars_series(base_dir=str(tmp_path))
    assert out.empty
    assert list(out.columns) == ["source", "symbol", "interval"]


def test_list_bars_series_finds_written_series(tmp_path):
    _write_bar(tmp_path, "BTCUSDT")
    _write_bar(tmp_path, "ETHUSDT")

    out = list_bars_series(base_dir=str(tmp_path))

    assert set(out["symbol"]) == {"BTCUSDT", "ETHUSDT"}


def test_upsert_is_idempotent(tmp_path):
    df = _sample_bars(["2024-01-01", "2024-01-02", "2024-01-03"])

    first = upsert_bars(df, "BTCUSDT", "binance", base_dir=str(tmp_path))
    second = upsert_bars(df, "BTCUSDT", "binance", base_dir=str(tmp_path))

    # Writing the same frame twice must not accumulate duplicate dates.
    assert first == second == 3


def test_upsert_merges_last_write_wins(tmp_path):
    upsert_bars(
        _sample_bars(["2024-01-01", "2024-01-02"], close=1.0),
        "BTCUSDT", "binance", base_dir=str(tmp_path),
    )
    rows = upsert_bars(
        _sample_bars(["2024-01-02", "2024-01-03"], close=9.0),
        "BTCUSDT", "binance", base_dir=str(tmp_path),
    )

    # 01-01, 01-02, 01-03 — the overlapping 01-02 merges rather than duplicating.
    assert rows == 3

    out = load_ohlcv_local("BTCUSDT", "binance", base_dir=str(tmp_path))
    # The second write's value for the shared day wins.
    assert out.loc[pd.Timestamp("2024-01-02", tz="UTC"), "close"] == 9.0


def test_upsert_leaves_no_temp_file(tmp_path):
    upsert_bars(_sample_bars(["2024-01-01"]), "BTCUSDT", "binance", base_dir=str(tmp_path))

    partition = _bars_path("BTCUSDT", "binance", "1d", base_dir=str(tmp_path)).parent
    assert list(partition.glob(".*.tmp")) == []


def test_a_failed_write_leaves_no_temp_file_behind(tmp_path, monkeypatch):
    # A half-written temp file surviving a crash is debris the next run has to step
    # around; cleaning up in `finally` means a failure costs nothing but the write.
    import qde.storage as storage_mod

    path = _bars_path("BTCUSDT", "binance", "1d", base_dir=str(tmp_path))

    def boom(*_a, **_k):
        raise ValueError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(ValueError):
        storage_mod._write_frame_atomic(_sample_bars(["2024-01-01"]), path)

    assert list(path.parent.glob(".*.tmp")) == []
    assert not path.exists(), "a failed write must not create the destination"


def test_a_pull_that_drops_a_column_is_reported(tmp_path, capsys):
    # concat fills the gap with NaN and last-write-wins keeps the incoming row, so a
    # source quietly dropping a field hollows out recent dates while the file keeps
    # its full column list — nothing downstream looks broken.
    upsert_bars(_sample_bars(["2024-01-01"]), "BTCUSDT", "binance", base_dir=str(tmp_path))
    capsys.readouterr()  # discard the first write's output

    thinner = _sample_bars(["2024-01-02"]).drop(columns=["volume"])
    upsert_bars(thinner, "BTCUSDT", "binance", base_dir=str(tmp_path))

    out = capsys.readouterr().out
    assert "schema_drift" in out
    assert "volume" in out


def test_a_stable_schema_says_nothing(tmp_path, capsys):
    # The warning is only useful if a normal night is silent.
    upsert_bars(_sample_bars(["2024-01-01"]), "BTCUSDT", "binance", base_dir=str(tmp_path))
    capsys.readouterr()
    upsert_bars(_sample_bars(["2024-01-02"]), "BTCUSDT", "binance", base_dir=str(tmp_path))
    assert "schema_drift" not in capsys.readouterr().out


def test_bars_watermark_none_when_absent(tmp_path):
    assert bars_watermark("BTCUSDT", "binance", base_dir=str(tmp_path)) is None


def test_bars_watermark_returns_latest_date(tmp_path):
    upsert_bars(
        _sample_bars(["2024-01-01", "2024-01-05", "2024-01-03"]),
        "BTCUSDT", "binance", base_dir=str(tmp_path),
    )

    assert bars_watermark("BTCUSDT", "binance", base_dir=str(tmp_path)) == pd.Timestamp(
        "2024-01-05", tz="UTC"
    )


def test_a_still_forming_bar_is_never_stored(tmp_path, monkeypatch):
    """Storing today's incomplete bar froze 16 consecutive days of corrupt data.

    The nightly runs at 00:30 UTC, so a fetch starting "the day after the watermark"
    landed on a daily bar thirty minutes old. It was stored as complete, the
    watermark advanced past it, and it was never re-fetched. Measured on the live
    lake: binance BTCUSDT 2026-08-19 held volume 150.4 against a true 29,054.3.
    """
    import pandas as pd

    from qde.storage import load_ohlcv_local, update_ohlcv, upsert_bars

    today = pd.Timestamp.now(tz="UTC").normalize()
    yesterday = today - pd.Timedelta(days=1)

    def _frame(index, volume):
        return pd.DataFrame(
            {"open": [1.0] * len(index), "high": [2.0] * len(index),
             "low": [0.5] * len(index), "close": [1.5] * len(index),
             "volume": [volume] * len(index)},
            index=pd.DatetimeIndex(index, name="date"),
        )

    # Seed two days back so there is a watermark to advance from.
    upsert_bars(_frame([today - pd.Timedelta(days=2)], 10.0), "X", "src", base_dir=str(tmp_path))

    # The source offers a settled bar AND today's partial one.
    served = _frame([yesterday, today], 99.0)
    served.loc[today, "volume"] = 0.4  # the tell-tale sliver of a forming bar
    monkeypatch.setattr(storage, "load_ohlcv", lambda *a, **k: served)

    update_ohlcv("X", source="src", base_dir=str(tmp_path))

    stored = load_ohlcv_local("X", "src", "1d", str(tmp_path))
    assert today not in stored.index, "a bar for a day that has not closed must not be stored"
    assert yesterday in stored.index, "the settled bar should still land"


def test_the_watermark_day_is_refetched_so_a_partial_bar_self_heals(tmp_path, monkeypatch):
    # Starting at watermark+1 meant a bar stored while forming could never be
    # corrected. Starting at the watermark makes the next run overwrite it.
    import pandas as pd

    from qde.storage import load_ohlcv_local, update_ohlcv, upsert_bars

    today = pd.Timestamp.now(tz="UTC").normalize()
    yesterday = today - pd.Timedelta(days=1)

    def _frame(index, volume):
        return pd.DataFrame(
            {"open": [1.0] * len(index), "high": [2.0] * len(index),
             "low": [0.5] * len(index), "close": [1.5] * len(index),
             "volume": [volume] * len(index)},
            index=pd.DatetimeIndex(index, name="date"),
        )

    # A partial bar already on disk for yesterday (volume far too small).
    upsert_bars(_frame([yesterday], 0.4), "X", "src", base_dir=str(tmp_path))

    captured = {}

    def _loader(symbol, start=None, **kw):
        captured["start"] = start
        return _frame([yesterday], 5000.0)  # the complete version

    monkeypatch.setattr(storage, "load_ohlcv", _loader)
    update_ohlcv("X", source="src", base_dir=str(tmp_path))

    assert captured["start"] == str(yesterday.date()), "must re-fetch the watermark day"
    stored = load_ohlcv_local("X", "src", "1d", str(tmp_path))
    assert float(stored.loc[yesterday, "volume"]) == 5000.0, "partial bar must be overwritten"


def test_backfill_cannot_reintroduce_a_forming_bar(tmp_path):
    """The same defect reached the lake twice, by two different routes.

    `update_ohlcv` was fixed first; then a repair backfill re-stored today's partial
    bar, because it reaches the lake through `upsert_bars` directly. The rule lives
    in the primitive so no future entry point can miss it.
    """
    import pandas as pd

    from qde.storage import load_ohlcv_local, upsert_bars

    today = pd.Timestamp.now(tz="UTC").normalize()
    yesterday = today - pd.Timedelta(days=1)
    idx = pd.DatetimeIndex([yesterday, today], name="date")
    frame = pd.DataFrame(
        {"open": [1.0, 1.0], "high": [2.0, 2.0], "low": [0.5, 0.5],
         "close": [1.5, 1.5], "volume": [100.0, 0.4]},
        index=idx,
    )

    upsert_bars(frame, "X", "src", base_dir=str(tmp_path))
    stored = load_ohlcv_local("X", "src", "1d", str(tmp_path))
    assert yesterday in stored.index
    assert today not in stored.index, "a bar whose day has not closed must not be stored"


def test_an_hourly_bar_is_settled_once_its_hour_has_elapsed(tmp_path):
    # The rule is "the period has elapsed", not "not today" — an hourly bar from
    # three hours ago is final even though it is dated today.
    import pandas as pd

    from qde.storage import load_ohlcv_local, upsert_bars

    now = pd.Timestamp.now(tz="UTC").floor("h")
    old_hour, this_hour = now - pd.Timedelta(hours=3), now
    idx = pd.DatetimeIndex([old_hour, this_hour], name="date")
    frame = pd.DataFrame(
        {"open": [1.0, 1.0], "high": [2.0, 2.0], "low": [0.5, 0.5],
         "close": [1.5, 1.5], "volume": [10.0, 0.1]},
        index=idx,
    )

    upsert_bars(frame, "X", "src", interval="1h", base_dir=str(tmp_path))
    stored = load_ohlcv_local("X", "src", "1h", str(tmp_path))
    assert old_hour in stored.index, "a completed hour is settled"
    assert this_hour not in stored.index, "the current hour is still forming"
