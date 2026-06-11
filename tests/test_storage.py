import pytest

from pathlib import Path
from qde.storage import save_ohlcv


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
