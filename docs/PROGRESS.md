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
| 2 | Licensing audit | In progress (per-source fields done) |
| 3 | Group schemas | Done (docs/schemas/) |
| 4 | Registry + `BaseIngestor` | Done |
| 5 | Source expansion | In progress (Wave 1 #5 Coinbase code-done, deploying; Wave 2 #6 ccxt done) |
| 6 | Streaming and backfills | Done |
| 7 | Orchestration with Dagster | Not started |
| 8 | Transformations with dbt | Not started |
| 9 | Data quality | In progress (freshness + null checks live on VPS; microstructure checks added) |
| 10 | Observability | In progress (Discord health alerts live; webhook opt-in) |
| 11 | CI/CD | Not started |
| 12 | Catalogue and publishing | Not started |

**Current focus:** Phase 6 is complete and the platform is fully deployed. The
collector runs 24/7; the batch side has watermarked incremental loads, idempotent
upserts, and a group-level backfill CLI, and now runs nightly on the VPS with bars
published to R2 (durable + queryable like microstructure). Recent hardening:
memory-safe streaming compaction, and a `NoNewData` exception so a real fetch error
is no longer mistaken for "already up to date".

**Phase 4 (the registry, "little book") is complete.** The `qde.registry` package
declares binance / kraken / yfinance once each as a `SourceSpec`, folding in the
scattered `SYMBOL_MAP` (now `SourceSpec.symbols`, canonical→native) and the Phase 2
licensing decision (`redistributable` / `license_note` — yfinance is code-only), and
generates the `dim_sources` catalogue. The `qde.ingest` package — a `BaseIngestor`
ABC plus per-source subclasses — replaced the hand-written loaders with no behavior
change (byte-for-byte identical output, proven), and `load_ohlcv` is registry-driven.
The CLIs are wired in: `backfill --from-registry` seeds declared-but-unseeded series,
and `daily_update` logs registry drift. Adding a source is now one `SourceSpec` row
plus a small ingestor class.

**Phase 3 (group schemas) is done** (`docs/schemas/`), and **Phase 5 Wave 1 is
underway**: a **data-sourcing plan** (`docs/data-sources.md`) maps the owner's
two-model strategy + coverage to sources/groups/licensing with a build order, and
**FRED has landed locally and is fully deployed** — `series` storage, the
`FredIngestor`, a curated 26-series government macro spine, group-aware
`backfill`/`daily_update`, a BOM-robust `secrets/fred.env` loader, and
`sync.publish_series` (overwrite-and-keep, mirror of `publish_bars`, wired into
`qde.sync`'s `__main__`). **FRED is live on the VPS**: 26 series in R2, refreshed +
published nightly (via a read-only `./secrets` mount so the batch containers get the
key), queryable server-lessly from any client with `qde.lake` `FROM series`.
**Wave 1 #2 — the CBOE volatility complex (VIX/VVIX/SKEW EOD, `series`) is fully
deployed**: 23,519 rows live in R2, queryable server-lessly via `qde.lake`.
**Wave 1 #3 — CFTC COT positioning (`series`, multi-metric) is fully deployed**:
`CftcIngestor` + `cftc` `SourceSpec` (18 markets, TFF futures-only), 187,253 rows
live in R2 (227 series files now published nightly) and queryable server-lessly
via `qde.lake`. As the first multi-metric source it forced a real (backward-
compatible) infra change — the `series` view now unions the flat and metric
partition depths (DuckDB rejects a single glob over both). **Wave 1 #4 — Binance
perp funding (`series`, multi-metric) is fully deployed**: `BinanceFuturesIngestor`
+ `binancefut` `SourceSpec`, 42,874 rows live in R2 (233 series files now published
nightly), reusing COT's machinery unchanged (purely additive — no storage/lake
changes); OI and liquidations are scoped out (no usable public REST history — see
the checklist). **Wave 1 #5 — 2nd microstructure venue (Coinbase) is code-complete
+ live-verified locally**: the collector gained a `VenueAdapter` seam (Binance
behavior-unchanged, `CoinbaseAdapter` added). **NEXT (resume point): deploy
`collector-coinbase` to the VPS** (ff, rebuild, `up -d`; verify Coinbase reachable
from the EU box + files land in R2), then a schema/doc pass for the per-venue
microstructure shape.

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

Folded into the registry (Phase 4) rather than done as a separate pass: the
classification lives on each `SourceSpec` where the publishing job will read it.

- [x] Classify every current source as redistributable or not — done on the
      three current specs: binance/kraken redistributable, yfinance not (scrapes
      Yahoo; code-only source).
- [x] Populate `redistributable` / `license_note` per source — fields live on
      `SourceSpec`; every registered source carries a licence note.
- [ ] Document the two-halves product shape in the README
- [ ] `docs/licensing.md` written

## Phase 3 — Group schemas

Documented in `docs/schemas/`. Written ahead of source expansion because
FRED/ALFRED and the calendar are the first non-`bars` shapes the lake will hold
(see `docs/data-sources.md`).

- [x] `bars` schema — `docs/schemas/bars.md` (documents the implemented layout:
      one mutable file per series, OHLCV on a UTC `date`).
- [x] `series` schema — `docs/schemas/series.md`. Scalar `(date, value)` series,
      one file per `(source, series_id)`; optional `metric` dimension for
      multi-scalar sources; **bitemporal vintage extension** (`realtime_start/end`,
      `vintaged=true`) for ALFRED revision history. Metadata (units/frequency/
      licence) lives on the registry, per-series for FRED.
- [x] `events` schema (bitemporal) — `docs/schemas/events.md`. `scheduled_ts` vs
      `observed_ts`, `actual`/`forecast`/`previous`/`revision_seq`; the
      `observed_ts >= scheduled_ts` ordering test; `forecast` is the code-only
      consensus column (free sources give everything but it).
- [x] `microstructure` schema — `docs/schemas/microstructure.md` (documents the
      implemented layout: date-partitioned part files, per-kind columns).
- [x] Group-by-shape rationale — `docs/schemas/README.md` (why shape over asset
      class; how medallion + group compose). A formal `docs/adr/` entry can follow.

## Phase 4 — Registry + `BaseIngestor`

- [x] `SourceSpec` pydantic model (`qde.registry.spec`) — one declarative entry
      per source: group, canonical→native symbol map, intervals, page size, rate
      limit, DQ thresholds (`expected_daily_rows`, `null_tolerance`,
      `freshness_sla_minutes`), and the licensing decision. One definition, three
      consumers (config · DQ contract · catalogue).
- [x] Source registry, the little book (`qde.registry.sources`) — binance /
      kraken / yfinance declared once; `SYMBOL_MAP` folded in as
      `SourceSpec.symbols`. Accessors `get_spec` / `all_specs` / `SOURCES`. Tested
      (`tests/test_registry.py`, 8 tests): faithful superset of the seeded lake.
- [x] `dim_sources` generated from the registry — `dim_sources()` renders the
      specs as the catalogue table (one row per source, incl. the licensing gate).
- [x] `BaseIngestor` ABC (`qde.ingest.base`) — holds the shared machinery once:
      symbol translation (via the spec), the cursor pagination loop, and the
      empty-result → `NoNewData` contract. A source implements only `first_cursor`
      / `fetch_page` / `normalize`. Retry/backoff stays in `qde.loaders.http`.
- [x] Migrate Binance / Kraken / yfinance onto the pattern — the three loaders are
      now `BaseIngestor` subclasses in `qde.ingest`; the old hand-written modules
      are removed (kept in git history). `load_ohlcv` resolves source + symbol from
      the registry, then delegates to `get_ingestor(source)`. **No behavior change,
      proven**: old-vs-new produce byte-for-byte identical frames on the same
      canned payloads for all three sources; full suite 85 green.
- [x] Wire the registry into the backfill / daily-update CLIs. `declared_series()`
      exposes the registry's intended set. `qde.backfill --from-registry` enumerates
      it (so a declared-but-unseeded series is seeded, e.g. kraken ETHUSDT); the
      default stays lake-discovery. `qde.daily_update` logs registry drift
      (`registry_unseeded`) each run — informational, never touches the update loop
      or the deployed job's behavior. Verified: drift surfaces the 5 unseeded series.
- [x] Docs refreshed for the new layout: README structure tree + architecture
      diagram, and `notebooks/demo.ipynb` (the code walkthrough) now describe the
      `qde.registry` / `qde.ingest` design; the deleted `*_loader.py` / `symbols.py`
      references are gone. Also fixed stale README lines (loader tests are mocked;
      the daily update runs on the VPS cron, not Windows Task Scheduler).

## Phase 5 — Source expansion

Build order + licensing in `docs/data-sources.md`. Wave 1 (feed the owner's
two-model strategy, free + redistributable) is underway, starting with FRED.

- [x] **`series` storage** (`qde.storage`) — the scalar-series cousin of bars:
      `_series_path` / `upsert_series` / `series_watermark` / `list_series`, one
      mutable file per `(source, series_id)`, optional `metric` partition. The
      idempotent writer + watermark are factored (`_upsert_frame` / `_watermark`),
      shared with bars (behavior unchanged). `query()` now serves a `series` view
      too and pins the session TZ to UTC. Tested (`tests/test_series_storage.py`).
- [x] FRED macro series (`series`) — **DEPLOYED: 26 series live in R2, nightly refresh working.**
      `qde.ingest.fred.FredIngestor` (offset pagination, `"."` → `NaN` row-kept),
      registered, with a curated 26-series **government-only** (redistributable)
      macro spine on a `series` `SourceSpec`. `backfill` and `daily_update` now
      handle `--group series` (`storage.update_series`), loading the key
      BOM-robustly from `secrets/fred.env` via the new `qde.env.load_env_file`
      (also fixed `lake._load_local_env`). Live seed via the CLI: **50,289 rows
      across all 26 series** in the local lake. Offline tests for the ingestor,
      env loader, series backfill, and series daily-update.
      **`series` now publishes to R2 and is queryable there** —
      `sync.publish_series` mirrors `publish_bars` (overwrite the single mutable
      `series.parquet`, keep the local working copy; its `series.parquet` rglob also
      covers the optional `metric=` partition), wired into `qde.sync`'s `__main__`
      after the bars publish. `qde.lake` gained a `series_glob` (twin of `bars_glob`,
      `**` absorbs the optional `metric=` level) and a guarded `series` view, so the
      same `FROM series` SQL runs against the local lake and R2. Tested offline
      (publish: 6 cases incl. the metric partition + empty-group no-op; lake: glob
      defaults/narrowing + view registration); full suite 116 green.
      **Deployed to the VPS (2026-08-05).** `secrets/fred.env` placed on the box
      (deploy-owned, 600); the box was 16 commits behind, so a clean fast-forward
      brought the whole registry/ingest/series/publish stack current, image rebuilt,
      collector restarted. Seeded there via the CLI (`series=26 total_rows=50295`),
      published to R2 (`publish_series_complete published=26 failed=0`), and verified
      queryable **from the laptop over R2** (`qde.lake` `FROM series` → 26 series,
      50,295 rows, fresh). One deploy fix landed: the batch containers had no way to
      see the key (`secrets/` was unmounted and `daily_update` got no `-e`), so a
      read-only `./secrets:/app/secrets:ro` mount was added to `docker-compose.yml`
      (commit `44b557c`); the BOM-robust loader reads it at `/app/secrets/fred.env`.
      Verified the nightly path finds the key with no `-e` (`daily_update_complete
      updated=34 failed=0`, all 26 FRED series "already up to date").
- [x] **Volatility complex: VIX/VVIX/SKEW EOD from CBOE (`series`) — Wave 1 #2.**
      `qde.ingest.cboe.CboeIngestor` on a `series` `SourceSpec` (`name="cboe"`,
      identity symbol map). CBOE's CDN serves each index's *whole* history as one
      CSV (`{SYMBOL}_History.csv`) with no date parameter, so the ingestor
      downloads the file and narrows to `[start, end]` client-side; a caught-up
      slice is empty → `NoNewData`, reproducing FRED's "already up to date"
      signal for the incremental `update_series` path. One uniform rule handles
      the two CSV shapes (VIX carries OHLC, VVIX/SKEW a single value column):
      **date = first column, EOD level = last column** (CLOSE for VIX). No API
      key — the CSVs are public; `redistributable=True` for EOD levels (real-time
      feed + options data are not; re-verify before publishing). Offline tests
      (`tests/test_cboe_ingestor.py`, 7: shape, CLOSE-not-OPEN, single-column
      parse, start/end filtering, caught-up→NoNewData, bad-series→ValueError);
      full suite 123 green. **Seeded locally: 23,519 rows** — VIX 9,244 & SKEW
      9,199 (from 1990), VVIX 5,076 (from 2006), all through 2026-08-05,
      queryable via `FROM series`. **Deployed to the VPS (2026-08-06)**: clean
      fast-forward to `3739f08`, image rebuilt (no collector restart — batch-only
      change), seeded on the box (`series=3 total_rows=23519`), published to R2
      (`publish_series_complete published=29 failed=0` — 26 FRED + 3 CBOE), and
      verified **queryable from the laptop over R2** via `qde.lake` `FROM series`.
      No secret needed (public CDN), so simpler than FRED's deploy; the nightly
      `daily_update` now advances the CBOE watermarks and re-publishes.
- [x] **CFTC COT positioning (`series`, multi-metric) — Wave 1 #3.** The first
      source to use the schema's **`metric` partition**: `qde.ingest.cftc.CftcIngestor`
      pulls the CFTC public-reporting Socrata API (Traders in Financial Futures,
      futures-only, dataset `gpe5-46if`) for a curated 18-market positioning spine
      (equity index, the Treasury curve + funding, the dollar + FX majors, VIX, and
      CME BTC/ETH), each on a `cftc` `SourceSpec` whose symbol map is a real
      canonical→native map (friendly ticker `ES` → CFTC code `13874A`). `load`
      returns a **wide** frame — one column per trader-category metric (dealer /
      asset-mgr / leveraged / other / nonreportable long+short, plus open interest,
      11 in all) — which the new `storage.upsert_series_frame` splits into one
      `metric=` file per column, preserving the universal `(date, value)` contract
      per metric. SoQL filters by market+date server-side, so a caught-up pull is an
      empty page → `NoNewData` (like FRED). **Infra change COT forced:** DuckDB
      rejects a single glob spanning mixed hive depth ("Hive partition mismatch"),
      so the `series` view — in both `storage.query` and `qde.lake` — now **unions a
      flat glob (FRED/CBOE) with a metric glob (COT/perps) via `UNION ALL BY NAME`**
      (metric=NULL on the flat side); `series_watermark` gained a cross-metric scan
      (a market's metrics share report dates) and `update_series` routes through
      `upsert_series_frame` so one weekly fetch advances all metrics. Offline tests
      (ingestor 5; storage +4 incl. the mixed-depth union; lake +2); full suite 134
      green. **Seeded locally: 187,253 rows** across 18 markets × 11 metrics (history
      to 2006; RTY/BTC/ETH shorter), through the 2026-07-28 report, queryable via
      `FROM series WHERE source='cftc'` alongside FRED/CBOE. **Deployed to the VPS
      (2026-08-06)**: ff to `9a1717b`, image rebuilt (the view fix touches
      storage/lake), seeded on the box (18 markets, 187,253 rows), published to R2
      (`publish_series_complete published=227` — 26 FRED + 3 CBOE + 198 CFTC), and
      verified **queryable from the laptop over R2** — the mixed-depth union view
      works over httpfs, all three series sources coexisting in one `series` view.
      No secret (public Socrata); the nightly `daily_update` now advances the 18
      COT markets weekly and re-publishes.
- [x] **Crypto derivatives: Binance perp funding (`series`, multi-metric) — Wave 1 #4.**
      `qde.ingest.binance_futures.BinanceFuturesIngestor` pulls Binance USD-M
      perpetual funding history (public `fapi.binance.com/fapi/v1/fundingRate`) as a
      multi-metric series — per 8h settlement the `funding_rate` and settlement
      `mark_price` — reusing COT's machinery verbatim (`upsert_series_frame` +
      the mixed-depth union view). A new `binancefut` `series` `SourceSpec` (BTC/ETH/
      SOLUSDT, exchange-native → redistributable, no key), distinct from the spot
      `binance` bars source (one spec = one group). Time-cursor pagination mirrors
      the spot klines ingestor (page from last settlement + 1ms; short page stops);
      the endpoint filters by `startTime`, so a caught-up pull is empty →
      `NoNewData`. Offline tests (4: wide shape, value/NaN-markprice, multi-page
      pagination, caught-up→NoNewData); full suite 138 green. **Seeded locally:
      42,874 rows** (BTC 15,136 / ETH 14,668 / SOL 13,070 = settlements×2 metrics,
      history to the 2019 perp listing), queryable via `FROM series
      WHERE source='binancefut'` beside FRED/CBOE/CFTC. **Deployed to the VPS
      (2026-08-06)**: ff to `86d4cdb`, image rebuilt, seeded on the box (42,874
      rows), published to R2 (`publish_series_complete published=233` — 26 FRED +
      3 CBOE + 198 CFTC + 6 binancefut), verified **queryable from the laptop over
      R2**. No secret; the box (EU) reaches Binance fapi fine; the nightly
      `daily_update` now advances funding every 8h. Purely additive — reused COT's
      multi-metric machinery with no storage/lake/backfill changes. **Scoped out,
      with reason:** open interest has only ~30d of
      REST history (a forward-snapshot job, not a backfill) and liquidations have no
      public historical REST (`allForceOrders` 404s — a streaming `!forceOrder`
      concern). Funding is the one perp series with clean, complete public history.
- [~] **Extend microstructure to a 2nd venue: Coinbase (`microstructure`) — Wave 1 #5.**
      **Code complete + live-verified locally; VPS deploy pending.** The streaming
      collector was refactored behind a **`VenueAdapter` seam** (`qde.stream.venues`),
      mirroring the batch `BaseIngestor` split: the loop keeps everything
      venue-neutral (buffering, timed flush, reconnect/backoff, sequence-gap
      tracking, session markers, bronze layout) and each venue supplies an adapter
      (`native_symbol` / `ws_url` / `subscribe_frames` / `max_frame_bytes` /
      `rest_snapshots` / `route`). Binance moved onto the seam **behavior-unchanged**
      (`config.stream_names` + `parsers` untouched; the 20 stream tests stayed
      green). The new `CoinbaseAdapter` speaks Coinbase Exchange's public, no-auth
      feed (`wss://ws-feed.exchange.coinbase.com`) — channels `matches`→trades,
      `level2_batch`→depth (the full `level2` now needs auth), `ticker`→book_ticker,
      plus `heartbeat`. **Three protocol differences it absorbs, all live-confirmed
      by probe:** (1) subscription is a post-connect frame, not a URL; (2) the order
      book anchors **inline** (a >1 MiB full snapshot on every connect — trips the
      `websockets` default 1 MiB frame limit, so `max_frame_bytes` is raised to
      16 MiB — so the REST-snapshot loop is a no-op for Coinbase); (3) `l2update`
      diffs carry **no update id**, so per-message depth continuity is skipped (the
      gap check tolerates the missing ids) and depth re-anchors from the inline
      snapshot, while trades stay contiguous by `trade_id`, ticker orders by
      `sequence`, and `heartbeat` (captured as its own kind, `last_trade_id` +
      `sequence`) is the quiet-market liveness beacon. Symbols map to canonical
      `BTCUSDT` (Coinbase `BTC-USD` → same partition as Binance, `source=`
      distinguishes the USD/USDT book — the basis signal), same convention the bars
      layer uses. Bronze stays per-venue faithful (Coinbase sends string prices +
      ISO-8601 times; kept as sent, silver reconciles). Offline tests
      (`tests/test_stream_coinbase.py`, 15, built from live-captured payloads: symbol
      map, each parser, route dispatch + ack-ignore + error-raise, subscribe channel
      mapping, unsequenced-depth tolerance, trade-id gap, full-collector capture to
      bronze); full suite **172 green**, ruff + mypy clean. **Live-verified locally**
      via the real `python -m qde.stream` (QDE_SOURCE=coinbase): all six kinds
      captured to bronze under `source=coinbase` — trades (trade_id contiguous, 0
      gaps), depth, book_ticker, the 45k-level inline snapshot, heartbeat, session —
      Coinbase BTC/USD mid queried back beside Binance in one `symbol=BTCUSDT`
      partition. A `collector-coinbase` service was added to `docker-compose.yml`
      (same image, different `QDE_SOURCE`, shared `/data` lake so the one nightly
      compact+sync ships both venues; no secrets mount — public feed).
      **Deployed to the VPS (2026-08-07):** ff, rebuilt, `up -d`; both `collector`
      (binance) + `collector-coinbase` running, capturing all kinds × 3 symbols
      (verified via SSH). The two prod risks cleared — the >1 MiB inline snapshots
      come through the raised `max_frame_bytes` on the EU box (one snapshot per
      symbol), and Coinbase is reachable from Hetzner EU (no IP gate like Binance).
      **Remaining:** verify R2 `source=coinbase` after the first nightly sync
      (00:30 UTC 08-08 — same source-agnostic ship-and-prune path as Binance), then
      a schema/doc pass for the per-venue shape.
- [x] **ccxt for unified exchange access (`bars`) — Wave 2 #6.** One shared
      `qde.ingest.ccxt_bars.CcxtIngestor` drives ccxt's unified `fetch_ohlcv`
      against any venue, so a new exchange is a registry row, not a module. Added
      **4 spot venues — coinbase, bybit, okx, kucoin** (each a `bars` `SourceSpec`
      whose name is the ccxt exchange id; the symbol map turns `BTCUSDT` into the
      venue's ccxt symbol, `BTC/USDT` or Coinbase's `BTC/USD`). Binance/Kraken keep
      their bespoke, byte-for-byte-validated ingestors. Pagination walks forward by
      time and, crucially, **probes past pre-listing windows**: several venues
      return `[]` for a window before the pair listed rather than clamping to the
      earliest candle, so the walk skips forward (by a step under any venue's page
      span) until it finds the listing — an empty page only stops the walk *after*
      data has started (caught up → `NoNewData`). Purely additive: bars is the
      original group, so `backfill`/`publish_bars`/the lake view carry the new
      venues unchanged. `ccxt` added to dependencies. Offline tests (6: shape,
      symbol translation incl. Coinbase USD, forward pagination, end filter,
      caught-up→NoNewData); full suite 157 green. **Seeded locally: 30,986 rows**
      across 12 new series — coinbase 8,887 & okx 8,396 & kucoin 8,234 (history to
      each venue's listing), bybit 5,469 (from 2021) — cross-venue BTC agrees to
      ~0.1%. **Deployed to the VPS (2026-08-07)**: ff to `c78c1de`, image rebuilt
      (ccxt dependency), all 4 venues reachable from the EU box, seeded there
      (30,986 rows), published to R2 (`publish_bars_complete published=20` — 8 +
      12 new), verified **queryable from the laptop over R2** (7 venues in `bars`).
      The nightly `daily_update` now advances all 12 via the same watermark path.
- [ ] Economic calendar (`events`) — FRED/ALFRED core free; forecast column code-only
- [ ] Equities (`bars`) — code-only; corporate actions are the pressure point.
      **Wave 2 #7, deferred (2026-08-07).** yfinance already covers equities
      (code-only, deployed). **Stooq is out** — it now serves a JavaScript
      proof-of-work bot challenge (`/__verify`), so an automated ingestor would be
      bypassing bot-detection (declined) and fragile regardless. The clean code-only
      path is **Tiingo** (documented REST EOD API), which needs a free API key like
      FRED — build + live-verify it once a key is available (`secrets/tiingo.env`),
      `redistributable=False` so it stays code-only.

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

- [x] **Registry-driven checks that run every night (`qde.checks`).** Walks the
      seeded lake and tests each series against the contract its `SourceSpec`
      already declares — the thresholds that configure the ingestors, now enforced.
      Two checks: **freshness** (is the series stale?) and **null rate** (does a
      column breach its `null_tolerance`?). Returns structured `Violation`s;
      `daily_update` logs them and feeds them to the alert.
- [x] **Freshness without per-series frequency metadata.** A fixed "N days old"
      rule is wrong the moment sources differ (daily CBOE, weekly CFTC, 8-hourly
      funding, monthly/quarterly FRED). Instead staleness is judged against each
      series' *own* recent spacing (a high percentile × a generous factor), so the
      check self-calibrates per series. The `3×` factor is deliberate: many series
      are dated by period *start* but published with a lag (June CPI lands mid-July,
      dated 06-01), so a ~2-month-old monthly observation is current, not stale —
      verified it does not false-positive on the live FRED monthly spine.
- [ ] Pandera schemas at the bronze boundary (row-level contract) — later
- [ ] dbt tests: not_null, unique, accepted_values — after dbt (Phase 8)
- [ ] Custom financial tests: OHLC coherence, gap limits, bitemporal ordering
- [ ] Data quality policy documented

### Microstructure checks

The existing checks are bars-shaped and do not carry over to tick data. The
streaming equivalents now run nightly via `checks.run_microstructure_checks`, wired
into `daily_update` beside the bars/series pass and surfaced through the *same*
`Violation` + Discord alert (group=`microstructure`, so no alert change). Runs over
the last **settled** day (yesterday UTC — today's partition is still being written;
on the VPS yesterday's is still local before the compact+sync). Validated live on
the box: the DuckDB book scan cleared **2.35M** Binance BTC quotes in 1.4s, 0
crossed / 0 negative / 0 gaps across ~4.3M quotes.

- [x] **Sequence continuity** — the collector already detects it *live* (writes
      `kind=gaps`); the nightly check **surfaces** those records (below).
- [~] Message rate per kind/symbol — the **activity** check covers the silent/
      partial-feed case (an active pair missing its trade tape or top-of-book is
      flagged); full per-kind rate baselining is still open.
- [ ] Latency percentiles (p50/p99), noting the clock-skew caveat — deferred (more
      a Phase-10 metric than a pass/fail check; Coinbase's ISO vs Binance's ms time
      needs per-venue handling).
- [x] **Crossed-book (bid > ask) and non-negative sizes** — DuckDB streaming scan
      per source (book_ticker is the chattiest kind, millions of rows/day), string
      prices via `TRY_CAST`, `union_by_name` across venues; a defect is error-level.
- [ ] Rows per partition per day, and part-file counts (small-files watch) —
      deferred (part-file counts are a post-compaction concern; the check runs
      pre-compaction).
- [x] **Surface gap records from `kind=gaps`** — per (source, symbol): a
      `sequence_jump` is missed data (error, with an estimated missed-message
      count), a `reconnect` is a known outage window (warn).

## Phase 10 — Observability

- [x] **Failure + staleness alerts (Discord webhook, `qde.alert`).** `daily_update`
      collects fetch-failure *details* (not just a count) and runs the DQ pass, then
      posts a compact health summary to a Discord webhook — but only when there is a
      failure or a violation, so a clean night stays silent (an alert that fires
      nightly trains you to ignore it). The webhook URL loads from a gitignored
      `secrets/discord.env` via the same read-only mount as the FRED key; with none
      set the sender is a logged no-op, so the pipeline is unchanged on a box that
      hasn't opted in. The nightly exit stays 0 so a DQ issue never blocks the
      compact/sync in `maintain.sh` — the *alert* is what surfaces it, not a crash.
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
