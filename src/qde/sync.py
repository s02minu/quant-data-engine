"""Sync: upload settled bronze files to R2, then prune them locally.

The collector writes to local disk for durability; this ships settled data to
object storage so the VPS disk stays flat and the box becomes disposable. A file
is deleted locally only after R2 confirms it holds a copy of the same size —
un-backfillable data is never removed on trust.

Only settled partitions (date before today, UTC) are synced, matching
compaction. Run order is compact then sync, so the uploaded files are the few
large compacted ones rather than the many raw part files, which keeps R2
operation counts near zero.

Credentials come from the environment, never the code:
    QDE_R2_ENDPOINT, QDE_R2_ACCESS_KEY_ID, QDE_R2_SECRET_ACCESS_KEY, QDE_R2_BUCKET
"""

import os
from datetime import UTC, datetime
from pathlib import Path

from qde.compact import partition_date
from qde.log import configure, get_logger

log = get_logger(__name__)


def r2_client_from_env():
    """Build an S3-compatible client for R2 from environment variables.

    boto3 is imported here rather than at module load so the sync logic can be
    exercised with a fake client without the dependency installed.
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["QDE_R2_ENDPOINT"],
        aws_access_key_id=os.environ["QDE_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["QDE_R2_SECRET_ACCESS_KEY"],
        region_name="auto",  # R2 ignores region but boto3 requires one
    )


def sync_bronze(base_dir: str, bucket: str, client, today=None) -> dict:
    """Upload settled bronze files to `bucket`, deleting each once it verifies.

    Args:
        base_dir: Root of the local lake (bronze lives under it).
        bucket: Target R2 bucket name.
        client: An S3-compatible client (boto3 or a stand-in for tests).
        today: UTC reference date; partitions on or after it are left alone.

    Returns:
        Summary with files uploaded, bytes uploaded, and files kept after a
        failed upload or size mismatch.
    """
    if today is None:
        today = datetime.now(UTC).date()

    base = Path(base_dir)
    bronze = base / "bronze"
    if not bronze.exists():
        return {"uploaded": 0, "bytes": 0, "failed": 0}

    uploaded = 0
    total_bytes = 0
    failed = 0

    for file in sorted(bronze.rglob("part-*.parquet")):
        pdate = partition_date(file.parent)
        if pdate is None or pdate >= today:
            continue  # today's active partition, or an unparseable path

        key = file.relative_to(base).as_posix()
        local_size = file.stat().st_size

        try:
            client.upload_file(str(file), bucket, key)
            remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        except Exception as exc:
            log.warning("sync_failed", key=key, error=type(exc).__name__, detail=str(exc))
            failed += 1
            continue

        if remote_size != local_size:
            # The remote copy does not match; keep the local file for a retry.
            log.warning("size_mismatch", key=key, local=local_size, remote=remote_size)
            failed += 1
            continue

        file.unlink()  # safe: R2 confirmed a copy of the same size
        uploaded += 1
        total_bytes += local_size
        log.info("synced", key=key, bytes=local_size)

    return {"uploaded": uploaded, "bytes": total_bytes, "failed": failed}


if __name__ == "__main__":
    configure()
    summary = sync_bronze(
        base_dir=os.getenv("QDE_BASE_DIR", "data"),
        bucket=os.environ["QDE_R2_BUCKET"],
        client=r2_client_from_env(),
    )
    log.info(
        "sync_complete",
        uploaded=summary["uploaded"],
        bytes=summary["bytes"],
        failed=summary["failed"],
    )
