from datetime import date
from pathlib import Path

import pandas as pd

from qde.compact import TEMP_NAME, compact_bronze, compact_partition

TODAY = date(2026, 7, 25)


def _write_part(base, kind, symbol, day, name, rows):
    """Create one part file with `rows` integer rows, return its partition dir."""
    partition = (
        Path(base)
        / "bronze"
        / "group=microstructure"
        / "source=binance"
        / f"kind={kind}"
        / f"symbol={symbol}"
        / f"date={day}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"n": range(rows)}).to_parquet(partition / name, index=False)
    return partition


def _part_files(partition):
    return sorted(partition.glob("part-*.parquet"))


def test_settled_partition_merges_to_one_file_preserving_rows(tmp_path):
    part = None
    for i, rows in enumerate([2, 3, 5]):
        part = _write_part(tmp_path, "trades", "BTCUSDT", "2026-07-24", f"part-{i}.parquet", rows)
    before = len(pd.read_parquet(part))

    compact_bronze(base_dir=str(tmp_path), today=TODAY)

    assert len(_part_files(part)) == 1
    assert len(pd.read_parquet(part)) == before == 10


def test_todays_partition_is_left_alone(tmp_path):
    part = None
    for i in range(2):
        part = _write_part(tmp_path, "trades", "BTCUSDT", "2026-07-25", f"part-{i}.parquet", 1)

    compact_bronze(base_dir=str(tmp_path), today=TODAY)

    # Still being written by the collector, so untouched.
    assert len(_part_files(part)) == 2


def test_compaction_is_idempotent(tmp_path):
    for i in range(3):
        part = _write_part(tmp_path, "depth", "ETHUSDT", "2026-07-24", f"part-{i}.parquet", 4)

    compact_bronze(base_dir=str(tmp_path), today=TODAY)
    compact_bronze(base_dir=str(tmp_path), today=TODAY)

    assert len(_part_files(part)) == 1
    assert len(pd.read_parquet(part)) == 12


def test_recovery_discards_temp_when_originals_survived(tmp_path):
    part = _write_part(tmp_path, "trades", "SOLUSDT", "2026-07-24", "part-0.parquet", 3)
    _write_part(tmp_path, "trades", "SOLUSDT", "2026-07-24", "part-1.parquet", 3)
    # A crash after writing the temp but before deleting originals.
    (part / TEMP_NAME).write_bytes(b"partial")

    compact_partition(part)

    assert not (part / TEMP_NAME).exists()
    assert len(_part_files(part)) == 1
    assert len(pd.read_parquet(part)) == 6


def test_recovery_finalises_temp_when_originals_gone(tmp_path):
    part = _write_part(tmp_path, "trades", "SOLUSDT", "2026-07-24", "part-0.parquet", 4)
    # A crash after deleting originals but before the rename: only the temp holds data.
    (part / "part-0.parquet").rename(part / TEMP_NAME)

    compact_partition(part)

    assert not (part / TEMP_NAME).exists()
    files = _part_files(part)
    assert len(files) == 1
    assert "compacted" in files[0].name
    assert len(pd.read_parquet(part)) == 4
