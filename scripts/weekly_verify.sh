#!/usr/bin/env bash
#
# Weekly deep verification: re-fetch every stored daily series and confirm the
# source still returns what it returned before, then check each one against a
# related instrument. See src/qde/weekly_verify.py for why these two checks do
# not live in the nightly.
#
# Deliberately a separate cron entry rather than a Sunday branch inside
# maintain.sh: this pass re-fetches every series over the network, so it is the
# slowest and least predictable job on the box. Sharing a schedule with the
# nightly would mean one rate-limited source could delay compaction and sync --
# the jobs that actually keep the lake correct -- for the sake of a check whose
# whole premise is that it can wait a week.

set -euo pipefail

cd "$(dirname "$0")/.."

set -a
. ./secrets/r2.env
set +a

echo "[$(date -u +%FT%TZ)] weekly verification start"

# Exit code is preserved: qde.weekly_verify exits non-zero when it finds anything
# error-severity, and cron mails that. Unlike the nightly, nothing runs after this
# step, so failing loudly costs nothing downstream.
#
# `|| status=$?` rather than a bare call: under `set -e` a non-zero exit would
# abort the script before the status line ran, so the one path this block exists
# to report would be the one path it never reached.
status=0
docker compose run --rm collector python -m qde.weekly_verify || status=$?

echo "[$(date -u +%FT%TZ)] weekly verification done (exit $status)"
exit $status
