import pytest

from pathlib import Path
from qde.storage import save_ohlcv, load_ohlcv_local, update_ohlcv


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
    save_ohlcv("BTCUSDT", source="binance", start="2024-01-01", end="2024-01-05", base_dir=str(tmp_path))

    # Load it back
    df = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))

    assert not df.empty
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]




def test_update_add_new_data(tmp_path):
    # Save a small initial dataset
    save_ohlcv( "BTCUSDT", source="binance", start="2024-01-01", end="2024-01-05", base_dir=str(tmp_path))

    # Check initial row count
    df_before = load_ohlcv_local("BTCUSDT", source="binance", base_dir=str(tmp_path))

    # Update - should fetch data after Jan 5
    update_ohlcv("BTCUSDT", source="binance", base_dir=str(tmp_path))

    # Check row count grew
    df_after = load_ohlcv_local( "BTCUSDT", source="binance", base_dir=str(tmp_path))
    assert len(df_after) > len(df_before)


