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
        if pdate is None:
            # Both compaction and this skip an unreadable date, so such a partition
            # would never be merged, never uploaded, and never pruned — data that
            # silently never leaves the box while quietly consuming its disk. The
            # layout makes this near-impossible, which is exactly why it would go
            # unnoticed if it ever happened.
            log.warning("unparseable_partition_date", path=str(file.parent))
            continue
        if pdate >= today:
            continue  # today's partition is still being written

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


def publish_bars(base_dir: str, bucket: str, client) -> dict:
    """Mirror the bars lake to `bucket`, overwriting; never prune locally.

    Bars differ from the streamed lake `sync_bronze` handles. Each series is a
    single `bars.parquet` that the batch job rewrites every day and still needs
    locally for the next incremental upsert. So this re-uploads the current file
    each run, overwriting the R2 copy, and keeps the local one. It is a handful
    of small files, so the repeated upload costs almost nothing.

    Args:
        base_dir: Root of the local lake (bronze lives under it).
        bucket: Target R2 bucket name.
        client: An S3-compatible client (boto3 or a stand-in for tests).

    Returns:
        Summary with files published, bytes uploaded, and failures.
    """
    base = Path(base_dir)
    bars_root = base / "bronze" / "group=bars"
    if not bars_root.exists():
        return {"published": 0, "bytes": 0, "failed": 0}

    published = 0
    total_bytes = 0
    failed = 0

    for file in sorted(bars_root.rglob("bars.parquet")):
        key = file.relative_to(base).as_posix()
        local_size = file.stat().st_size

        try:
            client.upload_file(str(file), bucket, key)
            remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        except Exception as exc:
            log.warning("publish_failed", key=key, error=type(exc).__name__, detail=str(exc))
            failed += 1
            continue

        if remote_size != local_size:
            # The remote copy is not intact; leave the local one and retry next run.
            log.warning("size_mismatch", key=key, local=local_size, remote=remote_size)
            failed += 1
            continue

        published += 1
        total_bytes += local_size
        log.info("published", key=key, bytes=local_size)

    return {"published": published, "bytes": total_bytes, "failed": failed}


def publish_series(base_dir: str, bucket: str, client) -> dict:
    """Mirror the scalar-series lake to `bucket`, overwriting; never prune locally.

    Twin of ``publish_bars``. A scalar series (FRED and the rest of the ``series``
    group) is a single ``series.parquet`` the batch job rewrites every day and
    still needs locally for the next incremental upsert, so it is republished each
    run with overwrite and the local copy is kept. Only the root and payload differ
    from bars; the publish protocol is identical.

    The glob is ``series.parquet`` under ``group=series``, which naturally covers
    the optional ``metric=`` partition that multi-scalar sources add (a perp's
    ``funding_rate`` vs ``open_interest``) -- those simply nest one level deeper.

    Args:
        base_dir: Root of the local lake (bronze lives under it).
        bucket: Target R2 bucket name.
        client: An S3-compatible client (boto3 or a stand-in for tests).

    Returns:
        Summary with files published, bytes uploaded, and failures.
    """
    base = Path(base_dir)
    series_root = base / "bronze" / "group=series"
    if not series_root.exists():
        return {"published": 0, "bytes": 0, "failed": 0}

    published = 0
    total_bytes = 0
    failed = 0

    for file in sorted(series_root.rglob("series.parquet")):
        key = file.relative_to(base).as_posix()
        local_size = file.stat().st_size

        try:
            client.upload_file(str(file), bucket, key)
            remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        except Exception as exc:
            log.warning("publish_failed", key=key, error=type(exc).__name__, detail=str(exc))
            failed += 1
            continue

        if remote_size != local_size:
            # The remote copy is not intact; leave the local one and retry next run.
            log.warning("size_mismatch", key=key, local=local_size, remote=remote_size)
            failed += 1
            continue

        published += 1
        total_bytes += local_size
        log.info("published", key=key, bytes=local_size)

    return {"published": published, "bytes": total_bytes, "failed": failed}


def publish_events(base_dir: str, bucket: str, client) -> dict:
    """Mirror the events lake to `bucket`, overwriting; never prune locally.

    Twin of ``publish_series``. An events calendar is a single ``events.parquet``
    the nightly job rewrites (a new revision is a new row) and still needs locally,
    so it is republished each run with overwrite and the local copy is kept. Only
    the root and glob differ from bars/series; the publish protocol is identical.

    Args:
        base_dir: Root of the local lake (bronze lives under it).
        bucket: Target R2 bucket name.
        client: An S3-compatible client (boto3 or a stand-in for tests).

    Returns:
        Summary with files published, bytes uploaded, and failures.
    """
    base = Path(base_dir)
    events_root = base / "bronze" / "group=events"
    if not events_root.exists():
        return {"published": 0, "bytes": 0, "failed": 0}

    published = 0
    total_bytes = 0
    failed = 0

    for file in sorted(events_root.rglob("events.parquet")):
        key = file.relative_to(base).as_posix()
        local_size = file.stat().st_size

        try:
            client.upload_file(str(file), bucket, key)
            remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        except Exception as exc:
            log.warning("publish_failed", key=key, error=type(exc).__name__, detail=str(exc))
            failed += 1
            continue

        if remote_size != local_size:
            # The remote copy is not intact; leave the local one and retry next run.
            log.warning("size_mismatch", key=key, local=local_size, remote=remote_size)
            failed += 1
            continue

        published += 1
        total_bytes += local_size
        log.info("published", key=key, bytes=local_size)

    return {"published": published, "bytes": total_bytes, "failed": failed}


def publish_gold(base_dir: str, bucket: str, client) -> dict:
    """Mirror the gold lake to `bucket`, overwriting; never prune locally.

    Twin of ``publish_series``/``publish_events``, for the dbt-materialized gold
    marts (``gold/group=bars/mart=.../data.parquet``, ``gold/dim_sources/...``).
    Gold is rebuilt in full each night by ``dbt build``, so it is republished with
    overwrite and the local copy kept. Gold lives under ``gold/`` (a medallion
    layer sibling of ``bronze/``), not inside ``bronze``, so the root differs; the
    publish protocol is identical.

    Args:
        base_dir: Root of the local lake (bronze/ and gold/ live under it).
        bucket: Target R2 bucket name.
        client: An S3-compatible client (boto3 or a stand-in for tests).

    Returns:
        Summary with files published, bytes uploaded, and failures.
    """
    base = Path(base_dir)
    gold_root = base / "gold"
    if not gold_root.exists():
        return {"published": 0, "bytes": 0, "failed": 0}

    published = 0
    total_bytes = 0
    failed = 0

    for file in sorted(gold_root.rglob("*.parquet")):
        key = file.relative_to(base).as_posix()
        local_size = file.stat().st_size

        try:
            client.upload_file(str(file), bucket, key)
            remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        except Exception as exc:
            log.warning("publish_failed", key=key, error=type(exc).__name__, detail=str(exc))
            failed += 1
            continue

        if remote_size != local_size:
            # The remote copy is not intact; leave the local one and retry next run.
            log.warning("size_mismatch", key=key, local=local_size, remote=remote_size)
            failed += 1
            continue

        published += 1
        total_bytes += local_size
        log.info("published", key=key, bytes=local_size)

    return {"published": published, "bytes": total_bytes, "failed": failed}


if __name__ == "__main__":
    configure()
    base_dir = os.getenv("QDE_BASE_DIR", "data")
    bucket = os.environ["QDE_R2_BUCKET"]
    client = r2_client_from_env()

    # Streamed microstructure: ship settled part files and prune them locally.
    micro = sync_bronze(base_dir=base_dir, bucket=bucket, client=client)
    log.info(
        "sync_complete",
        uploaded=micro["uploaded"],
        bytes=micro["bytes"],
        failed=micro["failed"],
    )

    # Batch bars: overwrite the R2 copy, keep the local working file.
    bars = publish_bars(base_dir=base_dir, bucket=bucket, client=client)
    log.info(
        "publish_bars_complete",
        published=bars["published"],
        bytes=bars["bytes"],
        failed=bars["failed"],
    )

    # Scalar series (FRED macro spine, etc.): same overwrite-and-keep as bars.
    series = publish_series(base_dir=base_dir, bucket=bucket, client=client)
    log.info(
        "publish_series_complete",
        published=series["published"],
        bytes=series["bytes"],
        failed=series["failed"],
    )

    # Events calendar (bitemporal releases): same overwrite-and-keep as series.
    events = publish_events(base_dir=base_dir, bucket=bucket, client=client)
    log.info(
        "publish_events_complete",
        published=events["published"],
        bytes=events["bytes"],
        failed=events["failed"],
    )

    # Gold marts (dbt-materialized): rebuilt nightly, same overwrite-and-keep.
    gold = publish_gold(base_dir=base_dir, bucket=bucket, client=client)
    log.info(
        "publish_gold_complete",
        published=gold["published"],
        bytes=gold["bytes"],
        failed=gold["failed"],
    )

    # The quality summary the Power BI dashboard reads; publish it too so it is
    # reachable off the VPS.
    summary_csv = Path(base_dir) / "quality_summary.csv"
    if summary_csv.exists():
        key = summary_csv.name
        try:
            client.upload_file(str(summary_csv), bucket, key)
            log.info("published", key=key, bytes=summary_csv.stat().st_size)
        except Exception as exc:
            log.warning("publish_failed", key=key, error=type(exc).__name__, detail=str(exc))
