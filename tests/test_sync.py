import os
from datetime import date
from pathlib import Path

import pandas as pd

from qde.sync import sync_bronze

TODAY = date(2026, 7, 25)


class FakeS3:
    """Minimal stand-in for a boto3 S3 client.

    Records uploads as {(bucket, key): size}. `truncate` simulates a corrupt
    upload by storing a wrong size, so the verification path can be tested.
    """

    def __init__(self, truncate=None):
        self.objects = {}
        self._truncate = truncate or set()

    def upload_file(self, filename, bucket, key):
        size = os.path.getsize(filename)
        if key in self._truncate:
            size -= 1
        self.objects[(bucket, key)] = size

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise KeyError(Key)
        return {"ContentLength": self.objects[(Bucket, Key)]}


def _write_part(base, kind, symbol, day, name, rows):
    partition = (
        Path(base) / "bronze" / "group=microstructure" / "source=binance"
        / f"kind={kind}" / f"symbol={symbol}" / f"date={day}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    file = partition / name
    pd.DataFrame({"n": range(rows)}).to_parquet(file, index=False)
    return file


def test_uploads_settled_files_and_prunes_them(tmp_path):
    a = _write_part(tmp_path, "trades", "BTCUSDT", "2026-07-24", "part-0.parquet", 3)
    b = _write_part(tmp_path, "trades", "BTCUSDT", "2026-07-24", "part-1.parquet", 3)
    client = FakeS3()

    summary = sync_bronze(str(tmp_path), "qde-lake", client, today=TODAY)

    assert summary["uploaded"] == 2
    # Uploaded to R2...
    assert len(client.objects) == 2
    # ...and removed locally.
    assert not a.exists() and not b.exists()


def test_r2_key_mirrors_the_hive_path(tmp_path):
    _write_part(tmp_path, "depth", "ETHUSDT", "2026-07-24", "part-0.parquet", 1)
    client = FakeS3()

    sync_bronze(str(tmp_path), "qde-lake", client, today=TODAY)

    (bucket, key), _ = next(iter(client.objects.items()))
    assert bucket == "qde-lake"
    assert key == (
        "bronze/group=microstructure/source=binance/"
        "kind=depth/symbol=ETHUSDT/date=2026-07-24/part-0.parquet"
    )


def test_todays_partition_is_not_synced(tmp_path):
    today_file = _write_part(tmp_path, "trades", "BTCUSDT", "2026-07-25", "part-0.parquet", 1)
    client = FakeS3()

    summary = sync_bronze(str(tmp_path), "qde-lake", client, today=TODAY)

    assert summary["uploaded"] == 0
    assert today_file.exists()  # still being written; left alone


def test_size_mismatch_keeps_local_copy(tmp_path):
    key = "bronze/group=microstructure/source=binance/kind=trades/symbol=SOLUSDT/date=2026-07-24/part-0.parquet"
    file = _write_part(tmp_path, "trades", "SOLUSDT", "2026-07-24", "part-0.parquet", 5)
    client = FakeS3(truncate={key})

    summary = sync_bronze(str(tmp_path), "qde-lake", client, today=TODAY)

    assert summary["failed"] == 1
    assert summary["uploaded"] == 0
    # Never delete a local copy that R2 did not confirm.
    assert file.exists()


def test_sync_is_idempotent(tmp_path):
    _write_part(tmp_path, "trades", "BTCUSDT", "2026-07-24", "part-0.parquet", 2)
    client = FakeS3()

    first = sync_bronze(str(tmp_path), "qde-lake", client, today=TODAY)
    second = sync_bronze(str(tmp_path), "qde-lake", client, today=TODAY)

    assert first["uploaded"] == 1
    assert second["uploaded"] == 0  # already gone locally
