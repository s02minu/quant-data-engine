from pathlib import Path

import pandas as pd

from qde.storage import (
    _bars_path,
    list_bars_series,
    load_ohlcv_local,
    query,
    save_ohlcv,
    update_ohlcv,
)


# Assert the called file exists on disk
def test_save_create_file(tmp_path):
    path = save_ohlcv(
        symbol="BTCUSDT",
        source="binance",
        start="2024-01-01",
        end="2024-01-05",
        base_dir=str(tmp_path),
    )
    assert Path(path).exists()


def test_load_reads_saved_file(tmp_path):
    # Save first
    save_ohlcv(
        "BTCUSDT", source="binance", start="2024-01-01", end="2024-01-05", base_dir=str(tmp_path)
    )

    # Load it back
    df = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))

    assert not df.empty
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]


def test_update_add_new_data(tmp_path):
    # Save a small initial dataset
    save_ohlcv(
        "BTCUSDT", source="binance", start="2024-01-01", end="2024-01-05", base_dir=str(tmp_path)
    )

    # Check initial row count
    df_before = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))

    # Update - should fetch data after Jan 5
    update_ohlcv("BTCUSDT", source="binance", base_dir=str(tmp_path))

    # Check row count grew
    df_after = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))
    assert len(df_after) > len(df_before)


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
