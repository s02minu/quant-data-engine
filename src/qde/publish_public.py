"""Publish the redistributable slice of the lake to the PUBLIC bucket + catalogue.

The public half of the two-halves product (ROADMAP §6/§12): only data the registry
marks ``redistributable=True`` is mirrored to a public R2 bucket, where anyone queries
it with their own DuckDB and **no credentials** — serve files, not queries (§5.1).

The gate is an **allowlist**: a source is published only if the registry names it and
marks it redistributable. Everything else is withheld, including sources the registry
has never heard of. That polarity is deliberate — see :func:`_publishable_sources` —
because publishing cannot be undone. It applies in both places data leaves:

- **Bronze**: a file is uploaded only if its ``source=`` partition is on the allowlist.
- **Gold**: the marts blend sources (``fct_bars_daily`` includes the yfinance ETFs),
  so their rows are filtered ``WHERE source IN (allowed)`` before upload — the private
  gold stays whole; only the public copy is trimmed.

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

import contextlib
import os
from pathlib import Path

import duckdb

from qde.catalogue import build_catalogue, write_catalogue
from qde.log import configure, get_logger
from qde.registry import all_specs

log = get_logger(__name__)

# Bronze groups published *from the local lake*. Microstructure is public too, but
# reaches the bucket by a different route — see _MIRRORED_PREFIX below.
_PUBLIC_GROUPS = ("bars", "series", "events")

# Microstructure is public too (decided 2026-08-16: the platform serves data; the
# strategy that reads it lives outside the repo). It is absent from _PUBLIC_GROUPS
# because it cannot be published from the local lake at all — see
# `mirror_private_prefix` — not because it is withheld.
_MIRRORED_PREFIX = "bronze/group=microstructure/"

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


# Where the consolidated files are staged before upload. Deliberately OUTSIDE
# `bronze/`, because the group globs below are recursive: a merged file written
# beside the partitions it was built from is read back into the *next* merge, and a
# single crash between writing and deleting it would leave every row duplicated,
# silently and permanently. Staging elsewhere makes that impossible rather than
# unlikely.
_STAGING = ".publish-staging"


def _publishable_sources() -> frozenset[str]:
    """Sources the registry explicitly permits republishing.

    An **allowlist**, not a list of exclusions, and the distinction matters more
    here than anywhere else in the codebase: publishing is the one irreversible
    action the platform takes. Filtering by "everything except the sources known to
    be forbidden" publishes anything the registry has not heard of — a spec that was
    retired while its files remained, a directory dropped in by hand, a source
    seeded before it was declared. Each of those is a plausible Tuesday, and each
    would have been served publicly, forever, with no error raised.

    Withholding something that should have been public is a bug fixable by the next
    nightly. Publishing something licensed is not fixable at all.
    """
    return frozenset(s.name for s in all_specs() if s.redistributable)


def _excluded_sources() -> list[str]:
    """Registered sources forbidden from republishing — reported, not enforced.

    The enforcement is :func:`_publishable_sources`; this exists so the catalogue
    and the run log can say *why* something is missing.
    """
    return sorted(s.name for s in all_specs() if not s.redistributable)


def _source_of(rel_parts: tuple[str, ...]) -> str | None:
    """The ``source=`` partition value in a lake-relative path, if it has one."""
    for part in rel_parts:
        if part.startswith("source="):
            return part.split("=", 1)[1]
    return None


def mirror_private_prefix(client, private_bucket: str, public_bucket: str, prefix: str) -> dict:
    """Copy objects straight from the private bucket to the public one.

    Everything else here publishes from the local lake, which works because bars,
    series and events are mutable single files that stay on disk. Microstructure is
    not: ``qde.sync`` ships each settled partition to R2 and **prunes it locally**,
    which is the only reason a 40 GB box can carry a feed this size. By the time
    publishing runs, the only microstructure on disk is the half-written current
    day — so publishing it from the local lake would quietly serve a fragment and
    report success. The bytes only exist in R2, so the copy has to happen there.

    Server-side: nothing travels through the VPS, so the size of the archive is
    irrelevant to the nightly's runtime and bandwidth.

    **Copy-only, never delete.** The public copy is an archive; a partition missing
    locally is expected, not a signal to withdraw anything already published.

    Objects already present at the same size are skipped, which is what keeps this
    affordable — without it every nightly would re-copy the entire history.
    """
    copied = skipped = failed = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=private_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key, size = obj["Key"], obj["Size"]
            try:
                if client.head_object(Bucket=public_bucket, Key=key)["ContentLength"] == size:
                    skipped += 1
                    continue
            except Exception:
                pass  # absent, or unreadable — either way, copy it
            try:
                client.copy_object(
                    Bucket=public_bucket,
                    Key=key,
                    CopySource={"Bucket": private_bucket, "Key": key},
                )
                copied += 1
            except Exception as exc:
                log.warning(
                    "mirror_failed", key=key, error=type(exc).__name__, detail=str(exc)
                )
                failed += 1
    log.info("mirror_complete", prefix=prefix, copied=copied, skipped=skipped, failed=failed)
    return {"copied": copied, "skipped": skipped, "failed": failed}


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
    allowed: list[str] | None = None,
) -> dict:
    """Mirror the redistributable lake + catalogue to the public bucket.

    Args:
        base_dir: local lake root.
        public_bucket: the PUBLIC R2 bucket name.
        client: an S3-compatible client (boto3 or a test stand-in).
        public_base_url: HTTPS origin embedded in catalogue sample queries.
        allowed: source names cleared for publication; defaults to the registry's
            redistributable set. Anything not named here is withheld, including
            sources the registry has never heard of.

    Returns:
        Summary counts: bronze files, gold marts, whether the catalogue landed, and
        how many uploads failed. Callers decide what to do about failures — the
        module entry point below exits non-zero on any.
    """
    base = Path(base_dir)
    publishable = _publishable_sources() if allowed is None else frozenset(allowed)
    excluded = _excluded_sources()
    bronze_ok = bronze_skipped = gold_ok = quality_ok = 0
    bronze_failed = gold_failed = quality_failed = 0

    con = duckdb.connect()
    staging = base / _STAGING
    staging.mkdir(parents=True, exist_ok=True)

    # --- bronze: mirror only files belonging to an allowed source ---
    for group in _PUBLIC_GROUPS:
        root = base / "bronze" / f"group={group}"
        for file in sorted(root.rglob("*.parquet")):
            rel = file.relative_to(base)
            source = _source_of(rel.parts)
            # A file with no source= partition cannot be attributed, so it cannot be
            # cleared for publication either.
            if source is None or source not in publishable:
                bronze_skipped += 1
                log.info("public_withheld", key=rel.as_posix(), source=source)
                continue
            if _upload(client, file, public_bucket, rel.as_posix()):
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
            merged = staging / f"{group}-all.parquet"
            body = " UNION ALL BY NAME ".join(reads)
            # IN, not NOT IN: an unrecognised source is withheld, and a NULL source
            # (a file whose hive path lacks the key) fails the test rather than
            # passing it, since `NULL IN (...)` is never true.
            in_list = ", ".join(f"'{x}'" for x in sorted(publishable)) or "''"
            try:
                con.execute(
                    f"COPY (SELECT * FROM ({body}) WHERE source IN ({in_list})) "
                    f"TO '{merged.as_posix()}' (FORMAT parquet)"
                )
                if _upload(client, merged, public_bucket, f"bronze/group={group}/all.parquet"):
                    bronze_ok += 1
                else:
                    bronze_failed += 1
            finally:
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
            merged = staging / f"{table}.parquet"
            sources = [str(p) for p in parts]
            try:
                con.execute(
                    f"COPY (SELECT * FROM read_parquet({sources!r}, union_by_name=true)) "
                    f"TO '{merged.as_posix()}' (FORMAT parquet)"
                )
                if _upload(client, merged, public_bucket, f"quality/{table}.parquet"):
                    quality_ok += 1
                else:
                    quality_failed += 1
            finally:
                merged.unlink(missing_ok=True)

    # --- gold: filter non-redistributable rows, then upload the trimmed copy ---
    for rel_key, source_col in _PUBLIC_MARTS.items():
        src = base / rel_key
        if not src.exists():
            continue
        tmp = staging / rel_key.replace("/", "__")
        in_list = ", ".join(f"'{x}'" for x in sorted(publishable)) or "''"
        try:
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{src.as_posix()}') "
                f"WHERE {source_col} IN ({in_list})) "
                f"TO '{tmp.as_posix()}' (FORMAT parquet)"
            )
            if _upload(client, tmp, public_bucket, rel_key):
                gold_ok += 1
            else:
                gold_failed += 1
        finally:
            tmp.unlink(missing_ok=True)

    # --- catalogue: generate + publish alongside the data ---
    catalogue = build_catalogue(base_dir, public_base_url)
    cat_path = base / "catalogue.json"
    write_catalogue(catalogue, str(cat_path))
    catalogue_ok = _upload(client, cat_path, public_bucket, "catalogue.json")

    with contextlib.suppress(OSError):
        staging.rmdir()  # empty unless a merge is mid-flight; never force it

    return {
        "bronze_published": bronze_ok,
        "bronze_skipped": bronze_skipped,
        "gold_published": gold_ok,
        "catalogue": catalogue_ok,
        "excluded": excluded,
        "allowed": sorted(publishable),
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

    # Microstructure is published straight from the private bucket, because the
    # local copy has already been pruned by the sync that precedes this. Guarded on
    # the private bucket name so an environment without it simply does nothing.
    private_bucket = os.getenv("QDE_R2_BUCKET")
    mirror = {"copied": 0, "skipped": 0, "failed": 0}
    if private_bucket:
        mirror = mirror_private_prefix(
            r2_client_from_env(),
            private_bucket,
            public_bucket,
            _MIRRORED_PREFIX,
        )

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
        mirrored=mirror["copied"],
        mirror_skipped=mirror["skipped"],
        failed=summary["failed"] + mirror["failed"],
    )

    # Exit non-zero when anything failed to land. Uploads are idempotent, so a
    # re-run is the fix; what must not happen is a nightly that publishes nothing
    # (an unscoped R2 token, an expired key) and still reports success — the failures
    # are per-file warnings, so without this the run looks green and the bucket is
    # stale. maintain.sh runs this last under `set -e`, so a non-zero exit here is
    # visible without putting the private-lake sync at risk.
    if summary["failed"] or mirror["failed"]:
        log.error(
            "publish_public_incomplete",
            failed=summary["failed"] + mirror["failed"],
            mirror_failed=mirror["failed"],
            bronze_failed=summary["bronze_failed"],
            gold_failed=summary["gold_failed"],
            catalogue=summary["catalogue"],
            hint="check the R2 token is scoped to the public bucket with write access",
        )
        raise SystemExit(1)
