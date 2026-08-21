# VPS deployment & operations

The always-on VPS (Hetzner, EU — chosen to avoid Binance's US-IP block) runs three
things:

1. **The streaming collector**, 24/7, via `docker compose up -d` — captures
   microstructure (trades, depth, book_ticker) to the local bronze lake.
2. **Daily lake maintenance**, via a cron job that runs `scripts/maintain.sh` at
   00:30 UTC.
3. **Weekly deep verification**, via a cron job that runs `scripts/weekly_verify.sh`
   — re-fetches every stored daily bar *and* scalar series, confirms each source
   still returns what it returned before, and checks peerless symbols against a
   related instrument.
   See [Weekly verification](#weekly-verification) below for why it is a separate
   entry rather than a Sunday branch inside `maintain.sh`.

Both use the one `qde-collector` image (built from the `Dockerfile`, which
`pip install .`s the whole package, so every dependency in `pyproject.toml` — batch
and streaming — is present). The container's base dir is `QDE_BASE_DIR=/data`,
mounted from `./data` on the host (see `docker-compose.yml`).

## What `maintain.sh` does

Five steps, all as one-off `docker compose run --rm collector …` containers.

**The order matters and is not arbitrary.** Compaction runs *first*, before the
data-quality pass inspects the lake. Run the other way round, the small-files check
looks at yesterday's partition while it is still thousands of uncompacted part files
and reports a failure this very script repairs ninety seconds later — it fired ten
times on 2026-08-16 for exactly that reason. Compacting first also earns the check its
meaning: afterwards, a small-files violation is real.

1. **Compaction** — `python -m qde.compact`: merges the many small microstructure part
   files in settled partitions into one. Non-fatal — a compaction problem must not
   abort the run before the sync, or settled data would never reach R2.
2. **Bars + series + events update** — `python -m qde.daily_update`: watermark-driven
   incremental pull of every stored bar *and* scalar series, a full-refresh of each
   seeded events calendar (release history, which revises), then rebuilds
   `quality_summary.csv`, then runs the **data-quality pass** (`qde.checks`:
   registry-driven freshness + null-tolerance checks over every seeded series, plus
   the bitemporal events check and the microstructure checks) and,
   if there is a fetch failure or a DQ violation, posts a **health alert** to a
   Discord webhook (`qde.alert`). The series half (FRED) needs `FRED_API_KEY`; the
   entry point loads it — and the optional `QDE_DISCORD_WEBHOOK` — via
   `load_source_secrets(extra=("discord.env",))`, reaching the container through the
   read-only `./secrets` mount
   (see below). The job still exits 0 even on a DQ violation, so a stale series
   never blocks the compact/sync that follow — the alert is what surfaces it.
3. **dbt build (gold)** — rebuilds the gold marts from the freshly-updated bronze
   into the mounted `/data/gold` lake, in one container invocation: regenerate the
   `dim_sources` seed from the current registry, `mkdir -p` the gold dirs (DuckDB's
   `COPY` won't create them), then `cd transform && DBT_PROFILES_DIR=. dbt build
   --vars 'lake_root: /data'`. Non-fatal — a transform failure must not block the
   sync of bronze. Needs no secrets (reads/writes local `/data`). See `transform/`.
4. **Sync** — `python -m qde.sync`: ships settled microstructure to R2 and prunes
   it locally (`sync_bronze`), mirrors the mutable bars files to R2 with overwrite
   while keeping the local copy (`publish_bars`), does the same for the scalar
   series (`publish_series`), the events calendar (`publish_events`), and the gold
   marts (`publish_gold`), and publishes `quality_summary.csv`.
5. **Public publish** — `python -m qde.publish_public`: mirrors the redistributable
   slice to the PUBLIC bucket, generates `catalogue.json`, and copies the
   microstructure archive **bucket-to-bucket**. That last part cannot read the local
   lake, because step 4 has just pruned it — the only microstructure left on disk is
   a half-written current day, so publishing from there would silently serve a
   fragment. Objects already present at the same size are skipped, so only the first
   run moves the full archive. Guarded on `QDE_R2_PUBLIC_BUCKET`; a missing
   `QDE_R2_BUCKET` is logged loudly rather than silently skipping the mirror.

   **Both bucket names must be passed into this container.** They are separate `-e`
   flags, and omitting `QDE_R2_BUCKET` once produced a run that published no
   microstructure at all while reporting `mirrored=0 failed=0` and exiting 0.

**Two secret directories, and the split is a security boundary, not tidiness.**

| Directory | Holds | Mounted into containers? |
|---|---|---|
| `secrets/` | source API keys (`fred.env`, `tiingo.env`), `discord.env` | **yes**, read-only |
| `secrets-infra/` | `r2.env` — R2 **write** credentials | **no** |

`secrets/` is bind-mounted into every container, so anything in it is readable by
every batch job. Publishing is the one irreversible action this platform takes, and
write access to the public bucket has no business sitting on disk inside a nightly
that only reads APIs and writes local Parquet — which is exactly what happened when a
convenience loader began reading the whole directory. Nothing inside a container ever
reads `r2.env`: `maintain.sh` sources it on the **host** and hands the values to the
sync/publish containers with explicit `-e` flags.

Splitting by *directory* rather than by filename means the mount stays simple and
adding a source with a key still needs no `docker-compose.yml` edit.

`maintain.sh` prefers `secrets-infra/r2.env`, falls back to `secrets/r2.env` with a
loud warning (so a half-migrated box keeps running), and exits non-zero if neither
exists. `weekly_verify.sh` loads no R2 credentials at all — it never uploads.

R2 credentials are read from `secrets-infra/r2.env` (gitignored, VPS-only) and passed
into the sync container with `-e`. Source keys (FRED) live in `secrets/fred.env`
and reach the batch containers through the **read-only `./secrets:/app/secrets:ro`
mount** in `docker-compose.yml` — the batch entry points call
`qde.env.load_source_secrets()` (WORKDIR is `/app`), which loads a file only when it
is named after a **registered source**. That is what keeps `r2.env` unloadable even
if it is present: "r2" is not a source. It is BOM-tolerant (a PowerShell-written
`.env` is UTF-16) and no-ops on a missing file. No secrets live in the repo, and
none are baked into the image.

## Weekly verification

`scripts/weekly_verify.sh` → `python -m qde.weekly_verify`. Three checks the nightly
structurally cannot do, because each needs a second look at the *source* rather than
at the lake:

- **`self_consistency`** — re-fetches settled history and confirms the source still
  returns the same numbers. Catches a feed that silently revises history, a
  non-deterministic backend, or pagination that drops rows depending on where the
  walk starts. All of those produce individually perfect frames; they are visible
  only by asking twice.
- **`series_self_consistency`** — the same question asked of scalar series, and it
  closes a blind spot that is structural rather than incidental. `update_series` is
  watermark-advanced: it fetches from the day after the newest stored observation, so
  a value already held is never requested again and a **revision to it is invisible
  to the nightly by design** — while macro data revises constantly. Full history is
  re-fetched (measured at 0.4-3.0s per source), so even a benchmark revision that
  restates decades is caught. Reported at `warn`: a statistical agency revising a
  first estimate is the data working as designed, not a broken feed, but the stored
  copy becomes an old vintage and the finding names its own fix (`qde.backfill`).
- **`proxy_check`** — for a symbol no other source carries, compares it against a
  *related* instrument. It cannot confirm a price, only detect gross decoupling: a
  frozen feed, a mislabelled ticker, a mirror serving noise. Reads the lake, so it
  costs nothing.

**Why a separate cron entry rather than a Sunday branch inside `maintain.sh`:**
this pass re-fetches every series over the network, making it the slowest and least
predictable job on the box. Sharing a schedule with the nightly would let one
rate-limited source delay compaction and sync — the jobs that actually keep the
lake correct — for a check whose entire premise is that it can wait a week.

Install it alongside the existing nightly (Sundays, well clear of 00:30 UTC):

```
30 3 * * 0 bash /home/<user>/quant-data-engine/scripts/weekly_verify.sh >> /var/log/qde-weekly.log 2>&1
```

Invoked via `bash` rather than directly, because the file is committed from Windows
and arrives mode `100644`: calling it as an executable would fail with "Permission
denied" on the first Sunday and be discovered a week later.

Unlike the nightly, this one **exits non-zero** when it finds anything
error-severity, so cron reports it. Nothing runs after it, so failing loudly costs
nothing downstream. A `warn` does not fail the run.

**What alerts and what does not.** Proxy findings come under two check names,
because they are two different statements:

- `proxy` — an *event*. A relationship that held for years stopped, or the feed is
  frozen. Alerts.
- `proxy_unavailable` — a standing *property of the lake*. No related instrument
  exists (GLD and TLT correlate with nothing else stored here), the history is too
  short, or the series stopped updating so its last rows are not the recent period.
  **Recorded to `dq_violations` but never alerted.** It would be true again every
  single week with nothing anyone could do about it, and a channel that cries wolf
  weekly gets muted — including the week it finally carries a frozen feed.

On the current lake a full pass checks **70 series (20 bars + 50 scalars) in ~45
seconds**. The `proxy_unavailable` pair (GLD, TLT) is recorded and stays silent;
a revised series *does* alert, because re-backfilling it is a real action someone
should take.

Results land in `quality/dq_runs` and `quality/dq_violations` like the nightly's,
tagged `cadence='weekly'` so the two passes are never averaged together — they
check different things, and pooling them would compare a nightly freshness sweep
with a weekly re-fetch and call the difference a trend. Rows written before that
column existed carry NULL and are all daily.

The public status page filters to `cadence = 'daily'`, so its run strip stays one
cell per night. Without that filter every Sunday would show two cells, and the
weekly's 03:30 row would outrank the nightly's 00:30 row as "last run" — reporting
weekly proxy findings as the latest nightly result. The filter is applied in JS
rather than SQL on purpose: the published file has no `cadence` column until the
first nightly writes one, and a SQL predicate on a missing column is a hard error
that would blank the page between deploying the site and the next nightly.

## Deploying a change

From `~/quant-data-engine` on the VPS:

```bash
git pull
docker compose build collector          # bakes the new src/ into the image
docker compose up -d                     # restart the live collector IF stream code changed
```

`docker compose run` (used by cron) picks up the freshly built image
automatically, so batch-only changes need only `build`, not a collector restart.

**Reclaim build cache before you log off.** Each rebuild leaves its layers behind,
and on a 40 GB box they add up faster than the data does — a deploy session once
left 24 GB of cache against 739 MB of actual lake, taking the disk to 79%. A full
disk stops ingestion, compaction and sync alike, so end every deploy session with:

```bash
docker builder prune -af
```

The nightly `check_disk` pass now alerts on this (warn 80%, error 90%, or under
3 GB free), but the cache is cheaper to drop than to be warned about.

## Seeding the bars lake (first time only)

`daily_update` only refreshes series that already exist; on a fresh VPS the bars
lake is empty, so seed it once with a backfill. From `~/quant-data-engine`:

```bash
for pair in "binance BTCUSDT" "binance ETHUSDT" "binance SOLUSDT" "kraken BTCUSDT" \
            "yfinance SPY" "yfinance QQQ" "yfinance GLD" "yfinance TLT"; do
  set -- $pair
  docker compose run --rm collector python -m qde.backfill --source "$1" --symbol "$2" --from 2010-01-01
done
```

Backfills are idempotent — re-run with an earlier `--from` any time to extend
history. (Alternatively, `scp -r data/bronze/group=bars <user>@<vps>:~/quant-data-engine/data/bronze/`
from the laptop copies an existing lake up instead.)

## Seeding the series lake (first time only)

The scalar-series group (FRED macro spine) is seeded the same way, but needs the
FRED key on the box first. Place `secrets/fred.env` (a single
`FRED_API_KEY=...` line) with deploy ownership and `600` perms — e.g. `scp` it to
`/tmp` and `install -o deploy -g deploy -m 600 /tmp/fred.env.staged
secrets/fred.env`. Then seed every declared series from the registry:

```bash
docker compose run --rm collector python -m qde.backfill \
  --group series --from-registry --from 2010-01-01
```

The key reaches the container through the `./secrets` mount, so no `-e` is
needed. Expect `backfill_complete group=series series=26`. Thereafter the nightly
`daily_update` advances each series' watermark and `qde.sync` publishes them to
R2 (`publish_series_complete published=26`).

## Seeding the events lake (first time only)

The `events` group — the bitemporal U.S. macro release calendar (`docs/schemas/
events.md`) — is FRED/ALFRED-backed, so it uses the same `secrets/fred.env` key
already on the box (via the `./secrets` mount). Seed it once, from the
genuine-revision (ALFRED) era:

```bash
docker compose run --rm collector python -m qde.backfill \
  --group events --from 2000-01-01
```

Expect `backfill_complete group=events series=11 total_rows≈33000` (~4,400 events
in one `us_macro` calendar file). Thereafter the nightly `daily_update` **full-refreshes**
the calendar (a revision is a new row for an already-stored period, so unlike
bars/series this re-pulls in full — cheap, the calendar is tiny) and `qde.sync`
publishes it to R2 (`publish_events_complete published=1`). It also runs the
bitemporal DQ check (`observed_ts >= scheduled_ts`, contiguous revisions), which
alerts via Discord like the others.

Verify from anywhere with the read-only token: `qde.lake` `FROM events`.

## Health alerts (optional)

The nightly job posts a Discord alert when a source fails to fetch or a
data-quality check trips (freshness / null tolerance) — and stays silent
otherwise. To enable it:

1. In your Discord server: **Server Settings → Integrations → Webhooks → New
   Webhook**, pick a channel, and **Copy Webhook URL**.
2. Place it on the VPS as a single line, deploy-owned and `600`, the same way as
   the FRED key:

   ```bash
   printf 'QDE_DISCORD_WEBHOOK=%s\n' 'https://discord.com/api/webhooks/…' \
     > /tmp/discord.env.staged
   install -o deploy -g deploy -m 600 /tmp/discord.env.staged secrets/discord.env
   ```

The `./secrets` mount delivers it to the batch container; `daily_update` loads it
with no `-e` plumbing. Without the file, alerting is a logged no-op and everything
else runs unchanged. Test it by hand with a run that has something to report, or
just wait for the next nightly.

## Verifying

Run the batch job by hand and watch the log:

```bash
docker compose run --rm collector python -m qde.daily_update
```

Expect `daily_update_complete updated=34 failed=0` (8 bar series + 26 FRED
series). After the next `maintain.sh` run, confirm the data reached R2 by querying
it from anywhere with `qde.lake` (DuckDB over R2, read-only token) — `FROM bars`,
`FROM series`, or a microstructure kind. The job **always exits 0 even if a series
fails**, so trust the data (max bar/series date, `quality_summary.csv`), not the
exit code.
