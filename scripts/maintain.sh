#!/usr/bin/env bash
#
# Daily lake maintenance: compact settled bronze partitions, then sync them to
# R2 and prune the local copies. Intended to run from cron on the VPS.
#
# R2 credentials are read from secrets/r2.env (gitignored, VPS-only) and passed
# into the one-off container. No secrets live in this file.

set -euo pipefail

# Run from the project root regardless of where cron invokes the script.
cd "$(dirname "$0")/.."

set -a
. ./secrets/r2.env
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
# add QDE_R2_PUBLIC_BUCKET and QDE_PUBLIC_BASE_URL to secrets/r2.env to activate.
if [ -n "${QDE_R2_PUBLIC_BUCKET:-}" ]; then
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
