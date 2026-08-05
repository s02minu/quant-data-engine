# VPS deployment & operations

The always-on VPS (Hetzner, EU — chosen to avoid Binance's US-IP block) runs two
things:

1. **The streaming collector**, 24/7, via `docker compose up -d` — captures
   microstructure (trades, depth, book_ticker) to the local bronze lake.
2. **Daily lake maintenance**, via a cron job that runs `scripts/maintain.sh` at
   00:30 UTC.

Both use the one `qde-collector` image (built from the `Dockerfile`, which
`pip install .`s the whole package, so every dependency in `pyproject.toml` — batch
and streaming — is present). The container's base dir is `QDE_BASE_DIR=/data`,
mounted from `./data` on the host (see `docker-compose.yml`).

## What `maintain.sh` does

Three steps, all as one-off `docker compose run --rm collector …` containers:

1. **Bars + series update** — `python -m qde.daily_update`: watermark-driven
   incremental pull of every stored bar *and* scalar series, then rebuilds
   `quality_summary.csv`. The series half (FRED) needs `FRED_API_KEY`; the entry
   point loads it from `secrets/fred.env`, which reaches the container via the
   read-only `./secrets` mount (see below).
2. **Compaction** — `python -m qde.compact`: merges the many small microstructure
   part files in settled partitions.
3. **Sync** — `python -m qde.sync`: ships settled microstructure to R2 and prunes
   it locally (`sync_bronze`), mirrors the mutable bars files to R2 with overwrite
   while keeping the local copy (`publish_bars`), does the same for the scalar
   series (`publish_series`), and publishes `quality_summary.csv`.

R2 credentials are read from `secrets/r2.env` (gitignored, VPS-only) and passed
into the sync container with `-e`. Source keys (FRED) live in `secrets/fred.env`
and reach the batch containers through the **read-only `./secrets:/app/secrets:ro`
mount** in `docker-compose.yml` — the batch entry points call
`qde.env.load_env_file("secrets/fred.env")` (WORKDIR is `/app`), which is
BOM-tolerant and no-ops if the file is absent. No secrets live in the repo, and
none are baked into the image.

## Deploying a change

From `~/quant-data-engine` on the VPS:

```bash
git pull
docker compose build collector          # bakes the new src/ into the image
docker compose up -d                     # restart the live collector IF stream code changed
```

`docker compose run` (used by cron) picks up the freshly built image
automatically, so batch-only changes need only `build`, not a collector restart.

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
