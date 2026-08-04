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
docker compose run --rm collector python scripts/daily_update.py

echo "[$(date -u +%FT%TZ)] compaction start"
docker compose run --rm collector python -m qde.compact

echo "[$(date -u +%FT%TZ)] sync start"
docker compose run --rm \
  -e "QDE_R2_ENDPOINT=$QDE_R2_ENDPOINT" \
  -e "QDE_R2_ACCESS_KEY_ID=$QDE_R2_ACCESS_KEY_ID" \
  -e "QDE_R2_SECRET_ACCESS_KEY=$QDE_R2_SECRET_ACCESS_KEY" \
  -e "QDE_R2_BUCKET=$QDE_R2_BUCKET" \
  collector python -m qde.sync

echo "[$(date -u +%FT%TZ)] maintenance done"
