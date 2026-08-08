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
  mkdir -p /data/gold/group=bars/mart=fct_bars_daily /data/gold/dim_sources
  cd transform && DBT_PROFILES_DIR=. dbt build --vars "lake_root: /data"
' || echo "[$(date -u +%FT%TZ)] dbt build failed; continuing to sync"

echo "[$(date -u +%FT%TZ)] compaction start"
# Non-fatal: a compaction problem (e.g. one oversized partition) must not abort
# the run before sync, or settled data and bars would never reach R2.
docker compose run --rm collector python -m qde.compact \
  || echo "[$(date -u +%FT%TZ)] compaction failed; continuing to sync"

echo "[$(date -u +%FT%TZ)] sync start"
docker compose run --rm \
  -e "QDE_R2_ENDPOINT=$QDE_R2_ENDPOINT" \
  -e "QDE_R2_ACCESS_KEY_ID=$QDE_R2_ACCESS_KEY_ID" \
  -e "QDE_R2_SECRET_ACCESS_KEY=$QDE_R2_SECRET_ACCESS_KEY" \
  -e "QDE_R2_BUCKET=$QDE_R2_BUCKET" \
  collector python -m qde.sync

echo "[$(date -u +%FT%TZ)] maintenance done"
