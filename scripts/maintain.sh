#!/usr/bin/env bash
#
# Daily lake maintenance: compact settled bronze partitions, then sync them to
# R2 and prune the local copies. Intended to run from cron on the VPS.
#
# R2 credentials are read from secrets-infra/r2.env (gitignored, VPS-only, and
# deliberately outside the directory mounted into containers) and passed
# into the one-off container. No secrets live in this file.

set -euo pipefail

# Run from the project root regardless of where cron invokes the script.
cd "$(dirname "$0")/.."

# --l2-only: keep the microstructure pipeline alive and nothing else.
#
# Live order-book capture is the one thing here that cannot be refilled -- no exchange
# serves historical L2 -- while bars, series and events can all be re-fetched later from
# source. So when the project is parked, this mode keeps the irreplaceable half running
# and stops the rest.
#
# It is NOT just "stop the cron". The collectors write continuously, and it is THIS
# script that compacts what they wrote, ships it to the private bucket and prunes the
# local copy. Collectors running with no maintenance fills a 38GB box.
#
# Skipped in this mode: the batch ingest (and with it the nightly DQ record), the dbt
# marts built over batch data, and the public publish -- the public bucket then keeps
# serving its last good copy and goes stale, which is the honest signal for a paused
# project. Compaction and sync still run, because those are what make the capture
# durable.
L2_ONLY=0
if [ "${1:-}" = "--l2-only" ]; then
  L2_ONLY=1
  echo "[$(date -u +%FT%TZ)] L2-ONLY mode: microstructure capture + sync only"
fi

# R2 credentials live in secrets-infra/, NOT secrets/.
#
# `secrets/` is bind-mounted read-only into every container (docker-compose.yml), so
# anything in it is readable by every batch job — including jobs that only read APIs
# and write local Parquet. Write access to the public bucket has no business being
# on disk in those. Nothing inside a container ever reads this file: it is sourced
# here on the HOST and handed to the sync/publish containers with explicit `-e`.
#
# The fallback keeps a half-migrated box running rather than failing the nightly, but
# says plainly that the file is still in the exposed location.
set -a
if [ -f ./secrets-infra/r2.env ]; then
  . ./secrets-infra/r2.env
elif [ -f ./secrets/r2.env ]; then
  . ./secrets/r2.env
  echo "[$(date -u +%FT%TZ)] WARNING: r2.env is still in secrets/, which is mounted"        "into every container. Move it to secrets-infra/ to keep write credentials"        "off the container filesystem."
else
  echo "[$(date -u +%FT%TZ)] ERROR: no r2.env in secrets-infra/ or secrets/" >&2
  exit 1
fi
set +a

# Compaction runs FIRST, before the checks in daily_update look at the lake.
#
# It only ever touches partitions dated before today, so the newest thing it can
# settle is yesterday -- which is also the only settled day still on local disk,
# since the sync below prunes each partition after uploading it. Run in the other
# order, daily_update's small-files check inspects yesterday while it is still
# thousands of uncompacted part files and reports a failure that this very script
# fixes ninety seconds later. It fired 10 times on 2026-08-16 for that reason alone.
#
# The ordering also earns something: because compaction has already run, a
# small-files violation now means compaction genuinely did not do its job.
echo "[$(date -u +%FT%TZ)] compaction start"
# Non-fatal: a compaction problem (e.g. one oversized partition) must not abort
# the run before sync, or settled data and bars would never reach R2.
docker compose run --rm collector python -m qde.compact \
  || echo "[$(date -u +%FT%TZ)] compaction failed; continuing to sync"

if [ "$L2_ONLY" = "0" ]; then

echo "[$(date -u +%FT%TZ)] bars update start"
docker compose run --rm collector python -m qde.daily_update

echo "[$(date -u +%FT%TZ)] dbt build start"
# Rebuild the gold marts from the freshly-updated bronze, into the mounted /data
# lake so the sync below ships them. One container invocation: regenerate the
# dim_sources seed from the current registry, ensure the gold dirs exist (DuckDB's
# COPY will not create them), then dbt build. Non-fatal -- a transform problem must
# not block the sync of bronze/series/events.
docker compose run --rm collector sh -c '
  python -c "from qde.registry import dim_sources; dim_sources().to_csv(\"transform/seeds/dim_sources_seed.csv\", index=False)"
  mkdir -p /data/gold/group=bars/mart=fct_bars_daily \
           /data/gold/group=series/mart=fct_series_features \
           /data/gold/group=events/mart=fct_events_revisions \
           /data/gold/group=microstructure/mart=fct_cross_venue_basis \
           /data/gold/dim_sources
  cd transform && DBT_PROFILES_DIR=. dbt build --vars "lake_root: /data"
' || echo "[$(date -u +%FT%TZ)] dbt build failed; continuing to sync"

else
  echo "[$(date -u +%FT%TZ)] L2-ONLY: skipping batch ingest and dbt build"
fi

echo "[$(date -u +%FT%TZ)] sync start"
docker compose run --rm \
  -e "QDE_R2_ENDPOINT=$QDE_R2_ENDPOINT" \
  -e "QDE_R2_ACCESS_KEY_ID=$QDE_R2_ACCESS_KEY_ID" \
  -e "QDE_R2_SECRET_ACCESS_KEY=$QDE_R2_SECRET_ACCESS_KEY" \
  -e "QDE_R2_BUCKET=$QDE_R2_BUCKET" \
  collector python -m qde.sync

# Public lake (Phase 12): mirror only the redistributable slice + catalogue.json to
# the PUBLIC bucket, so anyone can query it with their own DuckDB and no credentials.
# Guarded on QDE_R2_PUBLIC_BUCKET, so it is a no-op until that bucket is provisioned --
# add QDE_R2_PUBLIC_BUCKET and QDE_PUBLIC_BASE_URL to secrets-infra/r2.env.
if [ "$L2_ONLY" = "1" ]; then
  echo "[$(date -u +%FT%TZ)] L2-ONLY: skipping public publish (bucket keeps its last copy)"
elif [ -n "${QDE_R2_PUBLIC_BUCKET:-}" ]; then
  echo "[$(date -u +%FT%TZ)] public publish start"
  docker compose run --rm \
    -e "QDE_R2_ENDPOINT=$QDE_R2_ENDPOINT" \
    -e "QDE_R2_ACCESS_KEY_ID=$QDE_R2_ACCESS_KEY_ID" \
    -e "QDE_R2_SECRET_ACCESS_KEY=$QDE_R2_SECRET_ACCESS_KEY" \
    -e "QDE_R2_PUBLIC_BUCKET=$QDE_R2_PUBLIC_BUCKET" \
    -e "QDE_R2_BUCKET=$QDE_R2_BUCKET" \
    -e "QDE_PUBLIC_BASE_URL=${QDE_PUBLIC_BASE_URL:-}" \
    collector python -m qde.publish_public
fi

echo "[$(date -u +%FT%TZ)] maintenance done"
