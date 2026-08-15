"""Publish the redistributable slice of the lake to the PUBLIC bucket + catalogue.

The public half of the two-halves product (ROADMAP §6/§12): only data the registry
marks ``redistributable=True`` is mirrored to a public R2 bucket, where anyone queries
it with their own DuckDB and **no credentials** — serve files, not queries (§5.1).
The non-redistributable sources (currently ``yfinance`` — code-only) are filtered out
two ways:

- **Bronze**: files under a ``source=<excluded>`` partition are skipped.
- **Gold**: the marts blend sources (``fct_bars_daily`` includes the yfinance ETFs),
  so their rows are filtered ``WHERE source NOT IN (excluded)`` before upload — the
  private gold stays whole; only the public copy is trimmed.

The ``catalogue.json`` (``qde.catalogue``) is generated and published alongside, so
the public bucket is self-describing.

Separate from ``qde.sync`` (the private-lake sync): a different bucket, the
redistributable filter, and gold row-filtering. Credentials come from the env — the
public bucket name and its HTTPS origin are their own vars so the public and private
buckets never get crossed:

    QDE_R2_ENDPOINT, QDE_R2_ACCESS_KEY_ID, QDE_R2_SECRET_ACCESS_KEY  (write token)
    QDE_R2_PUBLIC_BUCKET     — the public bucket name
    QDE_PUBLIC_BASE_URL      — its HTTPS origin (embedded in catalogue sample queries)
"""

import os
from pathlib import Path

import duckdb

from qde.catalogue import build_catalogue, write_catalogue
from qde.log import configure, get_logger
from qde.registry import all_specs

log = get_logger(__name__)

# Bronze groups eligible for the public lake. Microstructure is the owner's private
# research feed and is never published.
_PUBLIC_GROUPS = ("bars", "series", "events")

# Data-quality history (qde.dq_history). Published in full and unfiltered: these are
# our own operational records — which checks ran, what failed, how stale something was
# — not anyone else's licensed data, so the redistributable rule does not apply. A
# public quality record is a trust signal for a data product; silence is not.
_PUBLIC_QUALITY = ("dq_runs", "dq_violations")

# Partition depth per bronze group, for building the consolidated file. `series` is
# mixed: single-value sources (FRED, CBOE) sit at source/series_id/, multi-metric
# ones (CFTC COT, perps) add a metric= level — and DuckDB rejects mixed hive depth
# under a single glob, so those two are read separately and unioned by name.
_BRONZE_GLOBS = {
    "bars": ("**/*.parquet",),
    "series": ("*/*/series.parquet", "*/*/*/series.parquet"),
    "events": ("**/*.parquet",),
}

# Gold marts to publish, and the column to filter non-redistributable rows on. All
# three fct marts carry ``source``; the catalogue.json supersedes the dim_sources
# mart, so that one is not published as a file.
_PUBLIC_MARTS = {
    "gold/group=bars/mart=fct_bars_daily/data.parquet": "source",
    "gold/group=series/mart=fct_series_features/data.parquet": "source",
    "gold/group=events/mart=fct_events_revisions/data.parquet": "source",
}


def _excluded_sources() -> list[str]:
    """Source names the registry forbids republishing (redistributable=False)."""
    return sorted(s.name for s in all_specs() if not s.redistributable)


def _upload(client, path: Path, bucket: str, key: str) -> bool:
    """Upload one file, verifying the remote size matches. Returns success."""
    local_size = path.stat().st_size
    try:
        client.upload_file(str(path), bucket, key)
        remote_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except Exception as exc:
        log.warning("public_upload_failed", key=key, error=type(exc).__name__, detail=str(exc))
        return False
    if remote_size != local_size:
        log.warning("size_mismatch", key=key, local=local_size, remote=remote_size)
        return False
    log.info("public_published", key=key, bytes=local_size)
    return True


def publish_public(
    base_dir: str,
    public_bucket: str,
    client,
    public_base_url: str | None = None,
    excluded: list[str] | None = None,
) -> dict:
    """Mirror the redistributable lake + catalogue to the public bucket.

    Args:
        base_dir: local lake root.
        public_bucket: the PUBLIC R2 bucket name.
        client: an S3-compatible client (boto3 or a test stand-in).
        public_base_url: HTTPS origin embedded in catalogue sample queries.
        excluded: source names to withhold; defaults to the registry's
            non-redistributable set.

    Returns:
        Summary counts: bronze files, gold marts, whether the catalogue landed, and
        how many uploads failed. Callers decide what to do about failures — the
        module entry point below exits non-zero on any.
    """
    base = Path(base_dir)
    excluded = _excluded_sources() if excluded is None else excluded
    bronze_ok = bronze_skipped = gold_ok = quality_ok = 0
    bronze_failed = gold_failed = quality_failed = 0

    con = duckdb.connect()

    # --- bronze: mirror every file except excluded sources' partitions ---
    for group in _PUBLIC_GROUPS:
        root = base / "bronze" / f"group={group}"
        for file in sorted(root.rglob("*.parquet")):
            rel_parts = file.relative_to(base).parts
            if any(part == f"source={x}" for x in excluded for part in rel_parts):
                bronze_skipped += 1
                continue
            if _upload(client, file, public_bucket, file.relative_to(base).as_posix()):
                bronze_ok += 1
            else:
                bronze_failed += 1

        # One consolidated file per group. This is what the catalogue's sample query
        # points at — a glob cannot be expanded over plain HTTP, so without this the
        # advertised query fails for anyone who copies it.
        #
        # Read by glob rather than by file list, and filter on the hive-derived
        # `source` column, so the redistributable rule still holds. Depths differ per
        # group: `series` mixes source/series_id/ with source/series_id/metric/, and
        # DuckDB rejects mixed hive depth under one glob, so those are unioned the
        # same way qde.storage.query does it.
        reads = [
            f"SELECT * FROM read_parquet('{(root / pattern).as_posix()}', "
            f"hive_partitioning=true)"
            for pattern in _BRONZE_GLOBS[group]
            if any(root.glob(pattern))
        ]
        if reads:
            merged = root / "all.parquet"
            body = " UNION ALL BY NAME ".join(reads)
            in_list = ", ".join(f"'{x}'" for x in excluded) or "''"
            con.execute(
                f"COPY (SELECT * FROM ({body}) WHERE source NOT IN ({in_list})) "
                f"TO '{merged.as_posix()}' (FORMAT parquet)"
            )
            if _upload(client, merged, public_bucket, f"bronze/group={group}/all.parquet"):
                bronze_ok += 1
            else:
                bronze_failed += 1
            merged.unlink(missing_ok=True)

    # --- quality: mirror the history, plus a consolidated file per table ---
    #
    # The partitioned files are published for completeness, but nothing can actually
    # read them over plain HTTP: a glob like `quality/dq_runs/**/*.parquet` needs
    # directory listing, and an r2.dev URL has none (DuckDB fails with "Globs for
    # generic HTTP file are not supported"). So each table is also written as ONE
    # file at a stable path, which is what a browser client should point at. These
    # tables are small — one row per run — so a single file stays cheap for years.
    for table in _PUBLIC_QUALITY:
        parts = sorted((base / "quality" / table).rglob("*.parquet"))
        for file in parts:
            if _upload(client, file, public_bucket, file.relative_to(base).as_posix()):
                quality_ok += 1
            else:
                quality_failed += 1

        if parts:
            merged = base / "quality" / f"{table}.parquet"
            sources = [str(p) for p in parts]
            con.execute(
                f"COPY (SELECT * FROM read_parquet({sources!r}, union_by_name=true)) "
                f"TO '{merged.as_posix()}' (FORMAT parquet)"
            )
            if _upload(client, merged, public_bucket, f"quality/{table}.parquet"):
                quality_ok += 1
            else:
                quality_failed += 1
            merged.unlink(missing_ok=True)

    # --- gold: filter non-redistributable rows, then upload the trimmed copy ---
    for rel_key, source_col in _PUBLIC_MARTS.items():
        src = base / rel_key
        if not src.exists():
            continue
        tmp = base / (rel_key + ".public.tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        in_list = ", ".join(f"'{x}'" for x in excluded) or "''"
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{src.as_posix()}') "
            f"WHERE {source_col} NOT IN ({in_list})) "
            f"TO '{tmp.as_posix()}' (FORMAT parquet)"
        )
        if _upload(client, tmp, public_bucket, rel_key):
            gold_ok += 1
        else:
            gold_failed += 1
        tmp.unlink(missing_ok=True)

    # --- catalogue: generate + publish alongside the data ---
    catalogue = build_catalogue(base_dir, public_base_url)
    cat_path = base / "catalogue.json"
    write_catalogue(catalogue, str(cat_path))
    catalogue_ok = _upload(client, cat_path, public_bucket, "catalogue.json")

    return {
        "bronze_published": bronze_ok,
        "bronze_skipped": bronze_skipped,
        "gold_published": gold_ok,
        "catalogue": catalogue_ok,
        "excluded": excluded,
        "quality_published": quality_ok,
        "bronze_failed": bronze_failed,
        "gold_failed": gold_failed,
        "quality_failed": quality_failed,
        "failed": bronze_failed
        + gold_failed
        + quality_failed
        + (0 if catalogue_ok else 1),
    }


if __name__ == "__main__":
    from qde.sync import r2_client_from_env

    configure()
    base_dir = os.getenv("QDE_BASE_DIR", "data")
    public_bucket = os.environ["QDE_R2_PUBLIC_BUCKET"]
    summary = publish_public(
        base_dir=base_dir,
        public_bucket=public_bucket,
        client=r2_client_from_env(),
        public_base_url=os.getenv("QDE_PUBLIC_BASE_URL"),
    )
    log.info(
        "publish_public_complete",
        bronze=summary["bronze_published"],
        bronze_skipped=summary["bronze_skipped"],
        gold=summary["gold_published"],
        quality=summary["quality_published"],
        catalogue=summary["catalogue"],
        excluded=summary["excluded"],
        failed=summary["failed"],
    )

    # Exit non-zero when anything failed to land. Uploads are idempotent, so a
    # re-run is the fix; what must not happen is a nightly that publishes nothing
    # (an unscoped R2 token, an expired key) and still reports success — the failures
    # are per-file warnings, so without this the run looks green and the bucket is
    # stale. maintain.sh runs this last under `set -e`, so a non-zero exit here is
    # visible without putting the private-lake sync at risk.
    if summary["failed"]:
        log.error(
            "publish_public_incomplete",
            failed=summary["failed"],
            bronze_failed=summary["bronze_failed"],
            gold_failed=summary["gold_failed"],
            catalogue=summary["catalogue"],
            hint="check the R2 token is scoped to the public bucket with write access",
        )
        raise SystemExit(1)
