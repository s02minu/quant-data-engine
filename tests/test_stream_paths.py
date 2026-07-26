from datetime import UTC, datetime

from qde.stream.config import StreamConfig
from qde.stream.paths import bronze_path


def test_bronze_path_has_hive_partition_layout():
    cfg = StreamConfig()
    when = datetime(2026, 7, 23, 14, 3, 12, 123000, tzinfo=UTC)
    path = bronze_path(cfg, "trades", "BTCUSDT", when).as_posix()

    assert "bronze/group=microstructure/source=binance" in path
    assert "kind=trades/symbol=BTCUSDT/date=2026-07-23/" in path
    assert path.endswith("part-20260723T140312123Z.parquet")


def test_bronze_path_is_unique_per_flush():
    cfg = StreamConfig()
    a = bronze_path(cfg, "trades", "BTCUSDT", datetime(2026, 7, 23, 14, 3, 12, 100000, tzinfo=UTC))
    b = bronze_path(cfg, "trades", "BTCUSDT", datetime(2026, 7, 23, 14, 3, 12, 200000, tzinfo=UTC))
    assert a != b
