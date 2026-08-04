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

1. **Bars update** — `python -m qde.daily_update`: watermark-driven incremental
   pull of every stored bar series, then rebuilds `quality_summary.csv`.
2. **Compaction** — `python -m qde.compact`: merges the many small microstructure
   part files in settled partitions.
3. **Sync** — `python -m qde.sync`: ships settled microstructure to R2 and prunes
   it locally (`sync_bronze`), then mirrors the mutable bars files to R2 with
   overwrite while keeping the local copy (`publish_bars`), and publishes
   `quality_summary.csv`.

R2 credentials are read from `secrets/r2.env` (gitignored, VPS-only) and passed
into the sync container. No secrets live in the repo.

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

## Verifying

Run the batch job by hand and watch the log:

```bash
docker compose run --rm collector python -m qde.daily_update
```

Expect `daily_update_complete updated=8 failed=0`. After the next `maintain.sh`
run, confirm bars reached R2 by querying them from anywhere with `qde.lake`
(DuckDB over R2, read-only token) — the same way microstructure is queried. The
job **always exits 0 even if series fail**, so trust the data (max bar date /
`quality_summary.csv`), not the exit code.
