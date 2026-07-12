# Quant Data Platform — Evolution Plan

*From `quant-data-engine` (working pipeline) to a complete, production-grade data platform.*

---

## 1. Vision

The end state is a platform where a user can query clean, validated financial market data — crypto OHLCV, tick data, reference data, and eventually macro/equities — through an API or dashboard, without knowing or caring how the data got there. Behind the scenes, the platform ingests from multiple source types, stores everything in an open lakehouse format, transforms it with tested SQL models, and monitors its own health.

The project should demonstrate the full data engineering lifecycle as described in *Fundamentals of Data Engineering*: **generation → ingestion → storage → transformation → serving**, with the undercurrents of **data quality, orchestration, observability, and DataOps** running through all of it.

## 2. Guiding principles

Every addition to this project must pass three tests before it goes in:

**Decoupled architecture.** Components communicate through storage contracts (Parquet files, database tables), never through direct dependencies. If ingestion dies, transformation still works on yesterday's data. If the API goes down, ingestion keeps writing. The Parquet lake is the contract between all stages.

**Two-way doors.** Prefer reversible decisions. DuckDB can be swapped for ClickHouse. Parquet works with every engine on earth. Dagster assets can be migrated to Airflow DAGs. Avoid anything proprietary that locks the data in.

**No resume-driven development.** Every tool gets added because the project has a real problem that tool solves — and the README documents that reasoning. "I added Dagster when manual runs became error-prone" is an interview answer. "I added Kafka because it's popular" is a red flag.

## 3. Data sources

A complete platform demonstrates handling **all three ingestion patterns**, because each has different engineering challenges:

| Source type | Pattern | Examples | Engineering challenge |
|---|---|---|---|
| REST APIs | Batch (scheduled pulls) | Binance/Kraken OHLCV, Polygon tickers, yfinance ETFs | Pagination, rate limits, retries, incremental loading, backfills |
| WebSockets | Streaming (pushed events) | Binance/Kraken live trade feeds | Connection management, reconnection, buffering, micro-batching to Parquet |
| Files | Static / semi-static | Symbol reference CSVs, exchange calendars, macro data downloads | Schema drift, encoding issues, slowly changing dimensions |

The batch side already exists in `qde`. The streaming side is the biggest genuine skill gap to close — a WebSocket consumer that buffers live trades and flushes micro-batches to Parquet every N seconds teaches real streaming concepts (at-least-once delivery, late data, watermarking) without needing Kafka infrastructure.

## 4. Target architecture

```
  REST APIs      WebSockets      Files/CSV
      │              │               │
      └──────────────┼───────────────┘
                     ▼
        Python ingestion layer (qde)
        retries · validation · logging          ┌─────────────┐
                     │                          │   Dagster    │
                     ▼                          │ schedules,   │
        Parquet lake on Cloudflare R2           │ retries,     │
        bronze → silver → gold                  │ monitors,    │
                     │                          │ backfills    │
                     ▼                          └─────────────┘
        dbt + DuckDB transformations
        tested, documented SQL models
                     │
                     ▼
        FastAPI + dashboard (serving)
```

**The medallion pattern (bronze/silver/gold)** is the storage backbone:

- **Bronze** — raw API responses, exactly as received, partitioned by source/date. Never modified. This is your replay log: if a transformation bug is found, you rebuild everything downstream from bronze.
- **Silver** — cleaned, deduplicated, typed, schema-enforced data. One row per trade/candle, validated.
- **Gold** — analytics-ready models: OHLCV resampled to standard intervals, rolling volatility, spreads, features for research.

## 5. The phases

### Phase 0 — Harden the foundation (1–2 weeks)

Before adding anything new, make the existing code production-grade. This is what separates engineers from script-writers.

| Task | Tool | What it does / why |
|---|---|---|
| Unit + integration tests | **pytest** | The standard Python test framework. Test the pagination logic, retry behavior (mock 429s), and Parquet write/read round-trips. Interviewers *will* ask "how do you test this?" |
| Static typing | **mypy** | Type checker. Catches bugs before runtime; signals code quality maturity. |
| Linting + formatting | **ruff** | Extremely fast linter/formatter that replaced flake8+black+isort in modern stacks. One config in `pyproject.toml`. |
| Structured logging | **structlog** or stdlib `logging` | Replace `print()` with leveled, timestamped, JSON-capable logs. Essential once things run unattended. |
| Config management | **pydantic-settings** | Typed settings loaded from env vars/.env. No more scattered `os.getenv` calls. |

**Deliverable:** `pytest` green in CI, `ruff check` and `mypy` clean, all loaders logging structured events.

### Phase 1 — Lakehouse storage on Cloudflare R2 (1 week)

Move the Parquet lake from local disk to object storage. This is the decoupling move: once data lives in R2, ingestion, transformation, the API, and your laptop all read from the same source independently.

| Task | Tool | What it does / why |
|---|---|---|
| Object storage | **Cloudflare R2** | S3-compatible object storage with zero egress fees (the reason to pick it over S3 for a self-funded project — reading your own data costs nothing). Free tier: 10 GB. |
| S3-compatible access | **boto3** / **fsspec + s3fs** | DuckDB and Python both speak S3 natively. `duckdb` can query `s3://bucket/path/*.parquet` directly with httpfs. |
| Partitioning scheme | Hive-style paths | `bronze/source=binance/symbol=BTCUSDT/date=2026-07-10/part-0.parquet` — engines prune partitions automatically, so queries touching one day don't scan the whole lake. |

**Deliverable:** all loaders write to R2; DuckDB queries the lake remotely; a documented partitioning scheme in the README.

### Phase 2 — Ingestion expansion (2–3 weeks)

| Task | Tool | What it does / why |
|---|---|---|
| Unify exchange access | **ccxt** | One library, 100+ exchange APIs, consistent interface. Replaces the hand-written Binance/Kraken loaders — but keep the old ones in git history and mention in the README that you built pagination/retries by hand first, *then* adopted ccxt. That trajectory is the story interviewers want. |
| Live streaming | **websockets** (Python lib) | Async WebSocket client. Subscribe to Binance trade streams, buffer in memory, flush micro-batches to bronze Parquet every 30–60s. Handles reconnects and gap detection. |
| Incremental loading | watermark pattern | Store the last-loaded timestamp per source/symbol; each run pulls only new data. This plus idempotent writes (overwrite by partition) is *the* core batch engineering pattern. |
| Backfills | CLI command | `qde backfill --symbol BTCUSDT --from 2020-01-01` — restatements and history loads are half of real DE work. |

**Deliverable:** live trades streaming into bronze, incremental batch loads with watermarks, a backfill command.

### Phase 3 — Orchestration with Dagster (1–2 weeks)

Manual runs and cron don't scale past a couple of jobs. An orchestrator manages schedules, dependencies, retries, and gives you a UI showing exactly what ran, when, and why it failed.

| Consideration | Dagster | Airflow |
|---|---|---|
| Mental model | Software-defined **assets** ("this Parquet table exists and is fresh") | **Tasks** ("run this script at 6am") |
| Local dev experience | Excellent — `dagster dev`, instant UI | Heavy — needs a scheduler, webserver, metadata DB |
| Job market recognition | Growing fast | Still the most common keyword |
| Fit for this project | Natural — your pipeline *is* a graph of data assets | Works, but more boilerplate |

**Recommendation: Dagster.** Its asset model maps directly onto bronze/silver/gold, the local experience is dramatically better, and being able to *compare* it to Airflow intelligently in an interview ("I chose Dagster because my pipeline is asset-oriented; Airflow's task model would have meant X") is worth more than using Airflow badly. Airflow concepts (DAGs, sensors, backfills) transfer almost one-to-one.

**Deliverable:** all ingestion jobs as Dagster assets with schedules, retry policies, and freshness checks visible in the Dagster UI.

### Phase 4 — Transformations with dbt (2 weeks)

| Task | Tool | What it does / why |
|---|---|---|
| SQL transformation framework | **dbt-core + dbt-duckdb** | dbt turns SQL SELECT statements into version-controlled, tested, documented models with automatic dependency resolution. The single most in-demand analytics engineering tool. dbt-duckdb runs it all locally/against R2 with zero warehouse cost. |
| Staging models | dbt `staging/` | One model per bronze source: rename, cast, deduplicate → silver. |
| Marts | dbt `marts/` | Gold layer: `fct_ohlcv_1h`, `fct_daily_returns`, `dim_symbols`, rolling volatility, volume profiles. This is where your quant knowledge shows — the models should be things a researcher would actually query. |
| Documentation | `dbt docs generate` | Auto-generated lineage graph + column docs, hostable as a static site. Very impressive artifact to link in the README. |

**Deliverable:** full bronze→silver→gold lineage in dbt, docs site published (GitHub Pages), Dagster triggering `dbt build` after ingestion.

### Phase 5 — Data quality (1 week, then ongoing)

Data quality is what interviewers probe hardest, because it's what juniors skip.

| Task | Tool | What it does / why |
|---|---|---|
| Schema validation at ingestion | **Pandera** | Typed DataFrame schemas: "timestamp must be UTC, close > 0, no duplicate (symbol, ts)". Fails fast at the bronze boundary. Lighter than Great Expectations for a project this size — and *saying that* shows judgment. |
| Transformation tests | **dbt tests** | Built-in: `not_null`, `unique`, `accepted_values`, plus custom SQL tests ("no OHLC candle where high < low", "no gaps > 2 intervals in hourly data"). |
| Freshness monitoring | dbt source freshness / Dagster freshness policies | Alerts when a source hasn't delivered new data within its SLA — e.g., "BTCUSDT hourly should never be more than 2 hours stale." |

**Deliverable:** every silver/gold model has tests; a documented data quality policy in `docs/`; at least one custom financial-domain test (OHLC coherence is the classic).

### Phase 6 — Observability and monitoring (1 week)

| Task | Tool | What it does / why |
|---|---|---|
| Pipeline health | Dagster UI + alerts | Run history, failure notifications (email or Discord webhook — free and demo-friendly). |
| Metrics over the data itself | small gold "meta" models | Row counts per partition per day, ingestion lag, API error rates — queryable like any other table, chartable in the dashboard. Monitoring the *data*, not just the *jobs*, is a senior-level distinction. |

**Deliverable:** failure alerts wired up; a "pipeline health" page in the dashboard.

### Phase 7 — CI/CD with GitHub Actions (3–4 days)

| Task | What runs | Why |
|---|---|---|
| CI on every PR | `ruff check` → `mypy` → `pytest` → `dbt build --target ci` on sample data | Nothing broken merges. This is DataOps in practice. |
| CD | On merge to main: build Docker image, deploy API | Shows the full loop from commit to running service. |
| Scheduled fallback | A cron-triggered Actions workflow can run light ingestion jobs free of charge | Good stopgap before/alongside hosted Dagster. |

**Deliverable:** green CI badge in the README, automated deploy of the API service.

### Phase 8 — Serving layer: the platform (2–3 weeks)

This is the "users can query and use it" part — deliberately last, because it's only as good as everything beneath it.

| Task | Tool | What it does / why |
|---|---|---|
| Query API | **FastAPI** | Async Python API framework with auto-generated OpenAPI docs. Endpoints like `GET /ohlcv/{symbol}?interval=1h&from=...` querying gold Parquet via DuckDB. Add API-key auth and rate limiting (you now know rate limits from *both* sides). |
| Packaging | **Docker + docker-compose** | `docker compose up` brings up the API + Dagster locally. Reproducibility is the point. |
| Hosting | **Render / Hugging Face Spaces / Fly.io** | Free-tier hosting for the API and dashboard. The compute is tiny because DuckDB + R2 does the heavy lifting. |
| Dashboard | **Streamlit** (or Evidence) | A public page: pick a symbol, see candles, volatility, volume, data freshness. This is what non-technical people (and recruiters) actually click. |

**Deliverable:** a live URL where anyone can query the data through the API or explore it in the dashboard.

## 6. What we deliberately skip — and say why in the README

| Tool | Why it's skipped (the interview answer) |
|---|---|
| **Spark** | Data volume is gigabytes, not terabytes. DuckDB processes this on one machine faster than a Spark cluster spins up. Knowing when *not* to use Spark is the skill. |
| **Kafka** | One producer, one consumer, replayable sources. A WebSocket buffer + Parquet micro-batches delivers the same guarantees at this scale with 1% of the operational burden. Would revisit if multiple independent consumers needed the live stream. |
| **Kubernetes** | One small API container. docker-compose is honest; K8s here would be cosplay. |
| **Snowflake/BigQuery** | Cost, and DuckDB + Parquet on R2 is a genuine lakehouse — arguably the more interesting architecture to discuss. |

This table is itself a portfolio asset: it demonstrates architectural judgment, the rarest junior-level trait.

## 7. Suggested order and rough timeline

Phases 0→8 in order, roughly 3–4 months at part-time pace. Each phase merges to main working and documented before the next begins — the repo should *always* be in a demoable state. Suggested cadence: Phase 0–1 (July), 2–3 (August), 4–5 (September), 6–8 (October).

## 8. Interview talking points this project earns you

- "Walk me through your architecture" → the diagram + medallion story, decoupling via Parquet contracts.
- "How do you handle failures?" → retries with backoff at HTTP level, Dagster retry policies at job level, idempotent partition overwrites at data level.
- "How do you know your data is correct?" → Pandera at the boundary, dbt tests in transformation, freshness SLAs, OHLC coherence tests.
- "Why DuckDB and not X?" → single-node scale, open formats, two-way door.
- "What would you change at 100x the data?" → partitioned reads already in place; swap DuckDB for ClickHouse/Trino, promote the WebSocket buffer to Kafka, move ingestion to autoscaling workers. The architecture survives; only components swap. That's the decoupling payoff.

## 9. Repo structure (end state)

```
quant-data-platform/
├── src/qde/              # ingestion package (existing, hardened)
│   ├── sources/          # rest/, websocket/, files/
│   ├── storage/          # R2 + Parquet writers
│   └── cli.py            # backfill, run commands
├── orchestration/        # Dagster definitions (assets, schedules, sensors)
├── transform/            # dbt project (staging → marts)
├── api/                  # FastAPI service
├── dashboard/            # Streamlit app
├── tests/                # pytest suites
├── docs/                 # architecture, ADRs, data quality policy
├── .github/workflows/    # CI/CD
├── docker-compose.yml
└── pyproject.toml
```

Note this is a **monorepo with clear internal boundaries**, not microservices. At this scale, that's the honest and reviewable choice — the boundaries (folders + storage contracts) are what matter, not deployment separation.
