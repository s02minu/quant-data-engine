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


def test_the_temp_file_is_invisible_to_parquet_globs(tmp_path):
    # Readers across the platform glob `*.parquet` — the dbt basis mart reads
    # `date=*/*.parquet` straight off local bronze. A temp ending in `.parquet` is
    # matched by every one of them, so an interrupted compaction would be read
    # alongside the originals it was merging and double-count until recovery ran.
    assert not TEMP_NAME.endswith(".parquet")

    partition = tmp_path / "date=2024-01-01"
    partition.mkdir(parents=True)
    (partition / TEMP_NAME).write_bytes(b"partial")
    assert list(partition.glob("*.parquet")) == []


def test_a_temp_left_by_an_older_build_is_cleaned_up(tmp_path):
    # The previous name ended in .parquet, so a file left behind by an older build
    # sits inside those same globs with nothing left that knows to remove it.
    from qde.compact import _LEGACY_TEMP_NAME

    partition = tmp_path / "bronze" / "group=microstructure" / "date=2024-01-01"
    partition.mkdir(parents=True)
    (partition / _LEGACY_TEMP_NAME).write_bytes(b"partial")

    compact_bronze(base_dir=str(tmp_path), today=date(2024, 6, 1))
    assert not (partition / _LEGACY_TEMP_NAME).exists()


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


def test_merges_parts_with_mismatched_null_column(tmp_path):
    # The streaming merge must reconcile schemas: one part has an all-null column
    # (null type), another has it typed. A naive writer would reject the mismatch.
    part = (
        Path(tmp_path)
        / "bronze"
        / "group=microstructure"
        / "source=binance"
        / "kind=trades"
        / "symbol=XRPUSDT"
        / "date=2026-07-24"
    )
    part.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"n": [1, 2], "price": [None, None]}).to_parquet(
        part / "part-0.parquet", index=False
    )
    pd.DataFrame({"n": [3], "price": [1.5]}).to_parquet(part / "part-1.parquet", index=False)

    compact_bronze(base_dir=str(tmp_path), today=TODAY)

    out = pd.read_parquet(next(part.glob("part-compacted-*.parquet")))
    assert len(out) == 3
    # The null-typed column reconciled with the float one; the real value survived.
    assert out["price"].dropna().tolist() == [1.5]
