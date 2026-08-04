# Progress Tracker

Working checklist for the build-out described in [ROADMAP.md](ROADMAP.md). The
roadmap holds the *reasoning*; this file holds the *state*.

**Legend:** `[x]` done · `[~]` in progress · `[ ]` not started

---

## At a glance

| Phase | Title | Status |
|---|---|---|
| — | Foundation (the working pipeline) | Done |
| 0 | Harden the foundation | Done |
| 1 | Lakehouse storage on Cloudflare R2 | Done |
| 2 | Licensing audit | Not started |
| 3 | Group schemas | Not started |
| 4 | Registry + `BaseIngestor` | Not started |
| 5 | Source expansion | Not started |
| 6 | Streaming and backfills | Done |
| 7 | Orchestration with Dagster | Not started |
| 8 | Transformations with dbt | Not started |
| 9 | Data quality | Not started |
| 10 | Observability | Not started |
| 11 | CI/CD | Not started |
| 12 | Catalogue and publishing | Not started |

**Current focus:** Phase 6 is complete and the platform is fully deployed. The
collector runs 24/7; the batch side has watermarked incremental loads, idempotent
upserts, and a group-level backfill CLI, and now runs nightly on the VPS with bars
published to R2 (durable + queryable like microstructure). Recent hardening:
memory-safe streaming compaction, and a `NoNewData` exception so a real fetch error
is no longer mistaken for "already up to date". Earlier phases (2–5) and 7+ are
open; the registry (Phase 4) is the likely next step.

---

## Foundation — the working pipeline

- [x] Binance OHLCV loader (REST, pagination, epoch to UTC)
- [x] Kraken OHLCV loader (REST, cursor pagination)
- [x] yfinance OHLCV loader (MultiIndex handling)
- [x] Unified `load_ohlcv()` with source routing
- [x] Cross-source symbol mapping
- [x] HTTP retry helper with exponential backoff
- [x] Parquet storage: save, load, incremental update
- [x] DuckDB SQL over stored Parquet
- [x] Quality checks: gaps, duplicates, nulls, price sanity
- [x] Quality summary export + Power BI dashboard
- [x] Notebook demo

## Phase 0 — Harden the foundation

- [x] pytest suite (loaders, symbol mapping, storage round-trips, errors)
- [x] Mock API responses so tests run without internet: `tests/conftest.py`
      fixtures monkeypatch the two network boundaries (`requests.get` for
      Binance/Kraken, `yfinance.download`) with canned payloads. The four loader
      tests now run offline (verified green behind a dead proxy) and the full
      suite dropped from ~11s to ~4.5s.
- [x] `ruff` lint + format, configured in `pyproject.toml` (E/W/F/I/UP/B/SIM).
      Fixed real findings: dead imports, a duplicate `load_kraken_ohlcv` import,
      `timezone.utc` -> `datetime.UTC`, `try/except/pass` -> `contextlib.suppress`;
      formatted the codebase. 33 offline tests still green after.
- [x] `mypy` static typing, clean (config in `pyproject.toml`,
      `ignore_missing_imports`). Only 2 findings, both pandas-stubs false
      positives; resolved with targeted, documented `# type: ignore[code]`.
- [x] `structlog` structured logging for the unattended services (collector,
      compact, sync) via `qde.log`: leveled, ISO-timestamped, key-value events;
      console renderer by default, JSON when `QDE_LOG_FORMAT=json`. Entry points
      call `configure()`. (Batch loaders' user-facing prints left as-is.)
- [x] `pydantic-settings` typed config from env: `StreamConfig` is now a
      `BaseSettings` model. Every field is overridable under the `QDE_` prefix
      (was only 3 of 11 hand-wired), values are type-coerced and validated at
      construction (a bad `QDE_DEPTH_SPEED`/`QDE_FLUSH_SECONDS` fails at startup,
      not deep in the collector), and the hand-rolled `config_from_env()` is
      gone. A `NoDecode` + validator keeps the comma-separated env UX
      (`QDE_SYMBOLS=BTCUSDT,ETHUSDT`) that docker-compose already relies on.

## Phase 1 — Lakehouse storage on Cloudflare R2

- [x] R2 bucket (`qde-lake`) provisioned, credentials via env — a write token on
      the VPS, a separate read-only token for analysis; never hardcoded.
- [x] Data lands in R2 via **write-local-then-sync**, not direct-to-S3 writers:
      the collector writes to local disk for durability and `qde.sync` ships
      settled files with boto3, deleting locally only after a same-size check.
      Chosen over writing straight to object storage so a mid-flush crash can
      never lose un-backfillable data. Details in Lake maintenance below.
- [x] DuckDB `httpfs` queries the R2 lake directly (`qde.lake`): partition
      pruning + column pushdown, no server. Verified ~3.1M rows counted from R2.
- [x] Hive-style partitioning, `group` as outermost key:
      `bronze/group=microstructure/source=binance/kind=.../symbol=.../date=...`.
- [x] Partitioning scheme documented — layout in the README, the group-by-shape
      rationale in ROADMAP §3.3.

### Lake maintenance

- [x] Compaction (`qde.compact`): merges the many small part files in a settled
      partition into one. Only touches partitions dated before today, since the
      collector still writes the current day. Crash-safe via temp → delete →
      rename, with a recovery pass for interrupted runs. **Streams part files
      through one Arrow writer, a file at a time**, so peak memory stays flat
      regardless of partition size — a whole-partition pandas concat OOM-killed the
      job on the 3.7 GB VPS. Tested (incl. both recovery cases + schema
      reconciliation) and verified on real data: 51 files -> 17, rows unchanged.
- [x] Sync (`qde.sync`): uploads settled bronze files to R2 and deletes each
      locally only after a same-size check confirms the remote copy. Idempotent
      (a synced file is gone locally, so re-runs skip it); S3 client injected so
      tested offline with a fake. Credentials read from env, never hardcoded.
- [x] Bars publishing (`qde.sync.publish_bars`): bars are a single mutable file
      per series, so they are mirrored to R2 with **overwrite** (not shipped-and-
      pruned like microstructure) and the local working copy is kept for the next
      incremental upsert. `qde.sync` now runs both, and the quality-summary CSV is
      published too. Closes the "bars are local-only" gap. Tested offline.
- [x] Scheduled maintenance as a daily VPS cron job (00:30 UTC) via
      `scripts/maintain.sh`, now **deployed and running**. Runs **bars update →
      compact → sync**: `python -m qde.daily_update` (incremental bars), then
      microstructure compaction, then the sync that ships microstructure and
      publishes bars + the quality CSV. Compaction is non-fatal in the script so a
      hiccup can never block the sync/publish. Verified end-to-end on the VPS: all
      8 bar series published to R2 (`publish_bars_complete published=8 failed=0`)
      and queried back from the laptop via `qde.lake`. Runbook: `docs/deploy.md`.
- [ ] R2 retention — **on hold, deliberately.** Originally planned as "delete
      objects older than ~14 days" to cap storage cost. Decided against for now:
      raw microstructure is the primary backtest fuel and is irreplaceable once
      dropped, while storage is cheap (~$5/mo for a full year on R2, zero egress).
      Keep everything while the symbol set is small; revisit only if volume grows
      enough that the bill matters, and prefer tiering (cheaper storage class)
      over deletion. See ROADMAP §11 retention [open].
- [x] Query the R2 lake with DuckDB (`qde.lake`): `httpfs` + an R2 secret from a
      read-only analysis token (separate from the VPS write token; creds bound as
      parameters, loaded from a local gitignored `secrets/r2-read.env`). Verified:
      counted ~3.1M rows across kinds/symbols straight from R2, no server.

### Unify the legacy layout

The batch pipeline predates the medallion decision and writes a flat layout,
`data/ohlcv/<symbol>_<source>_<interval>.parquet`, while the stream collector
writes `data/bronze/group=.../...`. Two layouts cannot coexist as source count
grows; the flat one is retired.

- [x] Migrate existing OHLCV files to `bronze/group=bars/source=/symbol=/interval=`
      — one file per series, **no `date` partition** (daily bars are one row/day,
      so date-partitioning would spawn one-row files). Done via
      `scripts/migrate_ohlcv_to_bronze.py` (copy → verify identical → prune).
- [x] `storage.py`: `_bars_path` is the new source of truth; `query()` reads a
      single hive-partitioned `bars` view (source/symbol/interval as columns).
      Dead `_ohlcv_path` has now been removed.
- [x] `quality.py`: `build_quality_summary` discovers series from partition
      metadata via `list_bars_series`, not by splitting filenames
- [x] `scripts/daily_update.py`: same partition-metadata discovery
- [x] Notebooks updated for the `bars` view (`demo.ipynb` tracked;
      `live_demo`/`sql_practice` are gitignored, fixed locally). Power BI reads
      `data/quality_summary.csv`, whose path is unchanged

## Phase 2 — Licensing audit (gating)

- [ ] Classify every current source as redistributable or not
- [ ] Populate `redistributable` / `license_note` per source
- [ ] Document the two-halves product shape in the README
- [ ] `docs/licensing.md` written

## Phase 3 — Group schemas

- [ ] `bars` schema
- [ ] `series` schema
- [ ] `events` schema (bitemporal: `observed_ts` vs `scheduled_ts`)
- [ ] `microstructure` schema
- [ ] ADR: group-by-shape over group-by-asset-class

## Phase 4 — Registry + `BaseIngestor`

- [ ] `SourceSpec` pydantic model
- [ ] Source registry (the little book)
- [ ] `BaseIngestor` ABC holding retry, pagination, partitioning, writes
- [ ] Migrate Binance / Kraken / yfinance onto the pattern
- [ ] `dim_sources` generated from the registry

## Phase 5 — Source expansion

- [ ] ccxt for unified exchange access (`bars`)
- [ ] FRED macro series (`series`)
- [ ] Volatility complex: VIX and term structure (`series`)
- [ ] Economic calendar (`events`) — blocked on licensing
- [ ] Equities (`bars`) — corporate actions are the pressure point

## Phase 6 — Streaming and backfills *(current)*

### Websocket collector

- [x] **Step 1 — Capture contract**
  - [x] `StreamConfig`: symbols, kinds, depth speed, flush window
  - [x] `stream_names()` — Binance naming contained at one boundary
  - [x] `bronze_path()` — Hive-partitioned part-file layout
- [x] **Step 2 — Connection**
  - [x] Combined-stream URL from config
  - [x] Async connect + read loop
  - [x] Verified live against Binance
- [x] **Step 3 — Parsers**
  - [x] Route messages by `stream` field to the parser for their kind
  - [x] Flatten payload, preserving prices/quantities as raw strings
  - [x] Stamp local UTC receive-time (event time vs processing time)
  - [x] Feed latency observable as `received_at - event_time`
  - [x] Kinds captured: `trades`, `depth`, `book_ticker`
  - [x] `book_ticker` has no exchange timestamp, so `received_at` is its only
        time reference; latency reported as n/a rather than failing
- [x] **Step 4 — Buffer and flush**
  - [x] In-memory buffer per (kind, symbol) — one buffer maps to one partition
  - [x] Timed flush task running concurrently with the read loop
  - [x] Write Parquet part files via `bronze_path()`
  - [x] Flush on shutdown so buffered data is not lost
  - [x] Verified: files read back via DuckDB, partition keys resolved from paths
  - [ ] Known limitation: the Parquet write blocks the read loop, since `flush`
        has no await point. Harmless at current volume; move the write to a
        thread pool if the socket starts backing up.
- [x] **Step 5a — Reconnect and gap detection**
  - [x] Reconnect loop with exponential backoff, capped at 60s
  - [x] Buffers flushed before waiting out an outage
  - [x] Per-kind continuity rules: `trades` and `depth` contiguous and
        countable, `book_ticker` ordering only
  - [x] Backwards jumps treated as replays, not as missing data
  - [x] Gap records written to their own `kind=gaps` partition
  - [x] Sequence state reset on reconnect so one outage yields one record
  - [x] Verified: synthetic sequences detect correctly, live feed reports no
        false positives
  - [x] Reconnect path verified with the mocked socket in Step 6
- [x] **Step 5b — Snapshot anchoring**
  - [x] REST depth snapshot stored under its own `kind=snapshot` partition
  - [x] Snapshot taken on every connect, so each connection is anchored
  - [x] Periodic snapshots on `snapshot_seconds`
  - [x] Blocking HTTP reuses the batch retry helper, run via `asyncio.to_thread`
        so the socket keeps draining
  - [x] A failed snapshot is skipped, never fatal to a live capture
  - [x] Verified: diff messages split correctly into stale vs replayable
        against a snapshot anchor
  - [x] **Session-boundary gaps recorded.** Start/stop markers written to a
        `kind=session` partition; downtime is the span between one session's
        stop (or last data) and the next start. Chosen over persisting last-seen
        ids because a restart gap is a wall-clock outage, not a sequence jump.
        Stop is best-effort (skipped on SIGKILL); the next start still bounds it.
- [x] **Step 6 — Tests**
  - [x] Config, path, parser, and gap unit tests (pure, offline, deterministic)
  - [x] Mocked-socket collector test: a fake websocket yields scripted messages
        then raises to simulate a drop; no network needed
  - [x] Reconnect path verified: forced drop, resume, exactly one reconnect gap
        recorded, no spurious sequence jump across the seam
  - [x] 19 tests passing in ~3s with no internet, unlike the batch loader tests
- [x] **Step 7 — Run it for real**
  - [x] Entry point `python -m qde.stream`, config from env (precursor to
        pydantic-settings); runs indefinitely, `QDE_MAX_MESSAGES` bounds a test
  - [x] `websockets` added to dependencies — was undeclared, would break a
        clean install
  - [x] SIGTERM handled as graceful cancellation so `docker stop` flushes the
        buffer instead of dropping it
  - [x] Dockerfile, `.dockerignore`, compose with `restart: unless-stopped`,
        `stop_grace_period: 30s`, and `./data` mounted as a volume
  - [x] Built and running in Docker on a Hetzner VPS (EU) — EU location avoids
        Binance's US-IP restriction; box is outbound-only behind ufw
  - [x] Capture running continuously, detached from any local session
  - [x] Measured rate: ~0.5-1 GB/day for 3 symbols x all kinds; ~27k small
        files/day. ~3-6 weeks of local disk runway on a 40 GB box.

Note on volume: `book_ticker` fires on every change to the size resting at the
best bid or ask, not only on price moves, so it is the chattiest of the three
kinds. Relevant to the retention question in ROADMAP §11.

### Batch side

- [x] Watermark pattern: `bars_watermark()` reads the last stored date per series
      straight from the data (no sidecar ledger that could drift) — the high-water
      mark an incremental pull advances from. `update_ohlcv` now fetches only bars
      after it.
- [x] Idempotent partition overwrites: `upsert_bars()` merges an incoming frame
      into the series file, deduping by `date` last-write-wins, then writes via a
      temp→rename (crash-safe, no half-written Parquet). `save_ohlcv` and
      `update_ohlcv` route through it, so a repeated or overlapping load converges
      to one row per date instead of duplicating. *The* core batch pattern.
- [x] Group-level backfill CLI (`python -m qde.backfill`): re-pull a date range and
      upsert it, uniformly across sources. `--source` + `--symbol` bootstraps a new
      series; filters narrow the set; no filter refreshes every series in the lake.
      A failing series is logged and skipped, never fatal. Verified idempotent
      across process runs (two identical runs → identical row count).

## Phase 7 — Orchestration with Dagster

- [ ] Assets generated from the registry
- [ ] Partitioned by date, source as a dimension
- [ ] Schedules, retry policies, freshness checks

## Phase 8 — Transformations with dbt

- [ ] dbt-core + dbt-duckdb project
- [ ] Staging models per group x source (silver)
- [ ] Marts: OHLCV resampling, returns, volatility, microstructure features (gold)
- [ ] `dbt docs` lineage site

## Phase 9 — Data quality

- [ ] Pandera schemas at the bronze boundary, thresholds from the registry
- [ ] dbt tests: not_null, unique, accepted_values
- [ ] Custom financial tests: OHLC coherence, gap limits, bitemporal ordering
- [ ] Freshness SLAs per source
- [ ] Data quality policy documented

### Microstructure checks

The existing checks are bars-shaped and do not carry over to tick data. The
streaming equivalents:

- [ ] Sequence continuity per kind (`trade_id` contiguous, `update_id` increasing)
- [ ] Message rate per kind/symbol — a silent feed is indistinguishable from a
      quiet market without this
- [ ] Latency percentiles (p50/p99), noting the clock-skew caveat
- [ ] Crossed-book check (bid >= ask) and non-negative sizes
- [ ] Rows per partition per day, and part-file counts (small-files watch)
- [ ] Surface gap records from `kind=gaps` in the quality dashboard

## Phase 10 — Observability

- [ ] Failure alerts (Discord webhook)
- [ ] Metrics over the data: row counts per partition, ingestion lag, error rates
- [ ] Pipeline-health page

## Phase 11 — CI/CD

- [ ] CI on every PR: ruff, mypy, pytest
- [ ] dbt build against sample data in CI
- [ ] Deploy on merge

## Phase 12 — Catalogue and publishing

- [ ] Public R2 bucket, publishing job filtered on `redistributable`
- [ ] Rate limiting + Cloudflare CDN cache in front of the public bucket, so a
      heavy or malicious reader cannot run up Class B op costs on the account
- [ ] Catalogue of datasets, schemas, freshness, DQ stats, licence
- [ ] Docker packaging
- [ ] Streamlit dashboard
- [ ] Live public URL with a copyable DuckDB query

---

## Deliberately not doing

Spark, Kafka, Kubernetes, Snowflake/BigQuery, and serving query compute to users.
Each is skipped for a stated reason — see [ROADMAP.md](ROADMAP.md) §8.

## Open questions

Tracked in [ROADMAP.md](ROADMAP.md) §11: group taxonomy, whether `bars` survives
equities, microstructure retention, catalogue as service vs static artifact,
symbol normalization across venues, medallion vs groups, calendar source.
