# Progress Tracker

Working checklist for the build-out described in [ROADMAP.md](ROADMAP.md). The
roadmap holds the *reasoning*; this file holds the *state*.

**Legend:** `[x]` done · `[~]` in progress · `[ ]` not started

---

## At a glance

| Phase | Title | Status |
|---|---|---|
| — | Foundation (the working pipeline) | Done |
| 0 | Harden the foundation | Partial |
| 1 | Lakehouse storage on Cloudflare R2 | Not started |
| 2 | Licensing audit | Not started |
| 3 | Group schemas | Not started |
| 4 | Registry + `BaseIngestor` | Not started |
| 5 | Source expansion | Not started |
| 6 | **Streaming and backfills** | **In progress** |
| 7 | Orchestration with Dagster | Not started |
| 8 | Transformations with dbt | Not started |
| 9 | Data quality | Not started |
| 10 | Observability | Not started |
| 11 | CI/CD | Not started |
| 12 | Catalogue and publishing | Not started |

**Current focus:** the websocket collector (Phase 6). It is prioritised ahead of
earlier phases because streamed microstructure data is un-backfillable — every
day it is not running is data that cannot be recovered later.

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
- [ ] Mock API responses so tests run without internet
- [ ] `ruff` lint + format, configured in `pyproject.toml`
- [ ] `mypy` static typing, clean
- [ ] `structlog` structured logging across loaders
- [ ] `pydantic-settings` typed config from env

## Phase 1 — Lakehouse storage on Cloudflare R2

- [ ] R2 bucket provisioned, credentials handled via env
- [ ] Writers target S3-compatible storage (boto3 / fsspec)
- [ ] DuckDB `httpfs` querying `s3://` paths directly
- [ ] Hive-style partitioning, `group` as outermost key
- [ ] Partitioning scheme documented

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
- [ ] **Step 3 — Parsers**
  - [ ] Route messages by `stream` field (trade vs depth)
  - [ ] Flatten payload, preserving `p`/`q` as raw strings
  - [ ] Stamp local UTC receive-time (event time vs processing time)
- [ ] **Step 4 — Buffer and flush**
  - [ ] In-memory buffer per (kind, symbol)
  - [ ] Timed flush task running concurrently with the read loop
  - [ ] Write Parquet part files via `bronze_path()`
  - [ ] Flush on shutdown so buffered data is not lost
- [ ] **Step 5 — Durability**
  - [ ] Reconnect with exponential backoff
  - [ ] Gap detection via trade id / depth update id continuity
  - [ ] Gap markers recorded in the data
  - [ ] Periodic REST depth snapshot to anchor the diff stream
- [ ] **Step 6 — Tests**
  - [ ] Mocked socket: no network needed
  - [ ] Parser unit tests
  - [ ] Partition-path tests
- [ ] **Step 7 — Run it for real**
  - [ ] Dockerfile + compose with `restart: unless-stopped`
  - [ ] Deployed to an always-on host
  - [ ] Capture running continuously

### Batch side

- [ ] Watermark pattern: last-loaded timestamp per source
- [ ] Idempotent partition overwrites
- [ ] Group-level backfill CLI

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
