"""Compaction: merge many small bronze part files into fewer large ones.

The streaming collector writes one Parquet file per flush, producing tens of
thousands of tiny files a day. Querying that many files is slow, they compress
poorly, and each is a separate upload to object storage. Compaction rewrites the
part files in a settled partition as a single file.

Only *settled* partitions are touched: those whose date is before today (UTC).
The collector only ever writes to the current day, so past days receive no
further files and can be merged without racing an in-progress write.

Crash safety uses a temp-then-rename protocol. Each partition is merged into a
dot-prefixed temp file, the originals are deleted, then the temp is renamed into
place. A recovery pass resolves a temp left by an interrupted run: if the
originals survived the merge never committed, so the temp is discarded; if they
were already deleted the rename simply never ran, so the temp is finalized.
"""

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from qde.log import configure, get_logger

TEMP_NAME = ".compact.tmp.parquet"

log = get_logger(__name__)


def _stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")[:-3] + "Z"


def _recover_partition(partition_dir: Path) -> None:
    """Resolve a temp file left behind by an interrupted compaction."""
    tmp = partition_dir / TEMP_NAME
    if not tmp.exists():
        return
    if any(partition_dir.glob("part-*.parquet")):
        # Originals survived, so the merge never committed. Discard the partial;
        # a later run redoes it from the intact originals.
        tmp.unlink()
    else:
        # Originals were already deleted; only the rename was missed. Finish it.
        tmp.rename(partition_dir / f"part-compacted-{_stamp()}.parquet")


def compact_partition(partition_dir: Path) -> int:
    """Merge every part file in one partition into a single file.

    Part files are streamed through one writer, a file at a time, rather than
    concatenated in memory all at once. A chatty day (book_ticker fires on every
    best-bid/ask change) can be gigabytes, and the raw-string price/quantity
    columns are especially heavy once materialised in pandas, so a
    whole-partition concat can exhaust a small box (OOM). Reading and writing one
    Arrow table at a time keeps peak memory to roughly a single part file.

    Returns the number of files merged, or 0 if there was nothing to do.
    """
    _recover_partition(partition_dir)

    originals = sorted(partition_dir.glob("part-*.parquet"))
    if len(originals) < 2:
        return 0

    # Reconcile schemas from the file footers only (cheap, no data loaded) so a
    # column that is all-null in one part and typed in another still lines up.
    # ParquetFile reads the file as-is; unlike pq.read_schema/read_table it never
    # infers the hive partition columns (group/source/...) from the path.
    schema = pa.unify_schemas([pq.ParquetFile(f).schema_arrow for f in originals])

    tmp = partition_dir / TEMP_NAME
    writer = pq.ParquetWriter(tmp, schema)
    try:
        for original in originals:
            writer.write_table(pq.ParquetFile(original).read().cast(schema))
    finally:
        writer.close()

    # Originals are removed only after the merged file is fully written and
    # closed; a crash before this leaves them intact for the recovery pass.
    for original in originals:
        original.unlink()
    tmp.rename(partition_dir / f"part-compacted-{_stamp()}.parquet")

    return len(originals)


def partition_date(partition_dir: Path) -> date | None:
    """Read the UTC date from a `.../date=YYYY-MM-DD` partition path."""
    for part in partition_dir.parts:
        if part.startswith("date="):
            try:
                return date.fromisoformat(part[len("date=") :])
            except ValueError:
                return None
    return None


def compact_bronze(base_dir: str = "data", today: date | None = None) -> dict:
    """Compact every settled partition under the bronze lake.

    A partition is settled when its date is before `today` (UTC), since the
    collector only writes to the current day.

    Returns a summary of partitions compacted and files merged.
    """
    if today is None:
        today = datetime.now(UTC).date()

    bronze = Path(base_dir) / "bronze"
    if not bronze.exists():
        return {"partitions_compacted": 0, "files_merged": 0}

    # Directories holding part files, plus any left mid-recovery with only a temp.
    partition_dirs = {p.parent for p in bronze.rglob("part-*.parquet")}
    partition_dirs |= {p.parent for p in bronze.rglob(TEMP_NAME)}

    partitions_compacted = 0
    files_merged = 0
    for partition_dir in sorted(partition_dirs):
        pdate = partition_date(partition_dir)
        if pdate is None or pdate >= today:
            continue  # unparseable, or the still-active current day

        merged = compact_partition(partition_dir)
        if merged:
            partitions_compacted += 1
            files_merged += merged

    return {"partitions_compacted": partitions_compacted, "files_merged": files_merged}


if __name__ == "__main__":
    import os

    configure()
    summary = compact_bronze(base_dir=os.getenv("QDE_BASE_DIR", "data"))
    log.info(
        "compacted",
        partitions=summary["partitions_compacted"],
        files=summary["files_merged"],
    )
