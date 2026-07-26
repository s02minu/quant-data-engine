# Quant Data Platform — Evolution Plan

*From `quant-data-engine` (working pipeline) to a complete, production-grade data platform.*

> **Status: provisional.** This is a working hypothesis, not a contract. It is
> revised continuously as I work through *Fundamentals of Data Engineering*, the
> DataExpert.io bootcamp, and the wider literature. Phases, ordering, taxonomy,
> and tooling are all expected to change as I encounter better patterns. Open
> decisions are marked **[open]** and collected in §11.

---

## 1. Vision

The end state is a platform where an analyst can query clean, validated financial
data — crypto OHLCV, tick and order-book data, equities, macro series, the
volatility complex, economic calendar — without knowing or caring how the data
got there.

**The wider goal:** *skip the data engineering phase.*

A large amount of duplicated, unglamorous work sits in front of every
quantitative analysis: pulling from a dozen APIs, reconciling symbol
conventions, normalizing timestamp units, handling pagination, dealing with
revisions. Most analysts do not want to do this work, and paying a vendor to
avoid it is expensive. This platform does that work once, in the open, and
publishes the result.

Three consequences follow, and they reshape the original plan:

1. **The platform hosts data I do not personally use.** Order flow is *my*
   strategy. The platform must equally serve analysts trading macro, volatility,
   seasonality, or fundamentals. Coverage is a first-class objective, not a side
   effect.
2. **Variety, not volume, is the core engineering problem.** Twenty
   heterogeneous sources is a harder problem than a terabyte of homogeneous
   ones — and it is the problem this project actually has.
3. **Cost discipline is a hard constraint.** A platform that is expensive to host
   is a platform that gets switched off. This constrains both storage design and,
   more sharply, the serving model (§5).

The project still demonstrates the full lifecycle from *Fundamentals of Data
Engineering* — **generation → ingestion → storage → transformation → serving** —
with the undercurrents of **data quality, orchestration, observability, and
DataOps** running throughout.

## 2. Guiding principles

Every addition must pass these before it goes in:

**Decoupled architecture.** Components communicate through storage contracts
(Parquet files, database tables), never direct dependencies. If ingestion dies,
transformation still works on yesterday's data. If the API goes down, ingestion
keeps writing. The Parquet lake is the contract between all stages.

**Two-way doors.** Prefer reversible decisions. DuckDB can be swapped for
ClickHouse. Parquet works with every engine on earth. Dagster assets can migrate
to Airflow DAGs. Avoid anything proprietary that locks the data in.

**No resume-driven development.** Every tool gets added because the project has a
real problem it solves — and the README documents that reasoning. "I added
Dagster when manual runs became error-prone" is an interview answer. "I added
Kafka because it's popular" is a red flag.

**One definition, many consumers.** *(New.)* A source is defined once, in the
registry. Config, data quality thresholds, and the public catalogue all read from
that single definition. Duplicated source knowledge is the thing that makes
twenty-source platforms unmaintainable.

---

## 3. The core pattern: the little book

Adopted from `EcZachly/little-book-of-pipelines` — the *pattern*, not the stack
(Scala/Spark/Hive is irrelevant here).

The problem it solves is **variety**: many upstream sources that mean roughly the
same thing but arrive with different schemas, pagination semantics, rate limits,
and quality guarantees. At three sources this is manageable by hand — which is
why `qde` works today. At twenty it is not, and backfills become unbearable
because every source has its own idea of what a backfill means.

### 3.1 Source registry

A single Python registry replaces per-source bespoke configuration. Each entry is
a `SourceSpec` (pydantic) holding everything constant about that source:

```python
class SourceSpec(BaseModel):
    group: str                      # which shared schema it writes to (§3.3)
    name: str                       # "binance", "fred", "cboe"
    symbols: list[str]
    granularity: str
    max_rows_per_call: int
    rate_limit_per_min: int
    expected_daily_rows: int        # DQ: row-count anomaly threshold
    null_tolerance: dict[str, float]
    freshness_sla_minutes: int
    redistributable: bool           # see §6
    license_note: str
```

One definition, three consumers:

1. **Config** for the ingestor,
2. **Data quality contract** read by Pandera and dbt tests,
3. **A row in the published catalogue** (`dim_sources`).

This is what the little book calls *self-documenting data quality code*. It is
the difference between a config file and an architectural pattern — the DQ
thresholds are not written twice, so they cannot drift.

### 3.2 Abstract ingestor

`BaseIngestor` (ABC) holds retry, backoff, pagination loop, partitioning, and
write logic **once**. A new source implements only:

- `fetch_page(cursor) -> RawPage`
- `normalize(raw) -> DataFrame`  (conforming to its group's shared schema)

Adding a source becomes a registry row plus two small methods. The hand-written
Kraken cursor logic and the HTTP retry helper are exactly the code that gets
lifted into the base class.

### 3.3 Groups: partition by *shape*, not by asset class

The instinct is to group by asset class — crypto / equities / macro / volatility.
This is wrong. Group by **physical shape and access pattern**, because shape is
what determines schema, partitioning, and storage cost.

| Group | Shape | Examples | Volume |
|---|---|---|---|
| `bars` | OHLCV time series | crypto spot, equity daily, futures | Moderate |
| `series` | `(series_id, ts, value)` | VIX and the volatility complex, rates, FRED macro | Tiny |
| `events` | Scheduled, sparse, **bitemporal** | economic calendar, earnings dates | Tiny |
| `microstructure` | Tick / L2 order book | own order-flow research | **Enormous** |

VIX is not a special case. It is one row in the registry pointing at the `series`
group. That is the entire payoff of the pattern: *new instrument types stop being
new modules.*

**One group → one job.** Under Dagster, each group becomes a partitioned asset
with source as a dimension. Backfills become uniform —
`backfill(group="bars", source="binance", from=...)` — because every source in a
group writes the same schema against the same partition key. This is the direct
cure for the painful-backfill problem, and it is why the registry must land
*before* orchestration.

### 3.4 Bitemporality for event data

Economic releases get revised. A calendar table storing only the *current* value
of a release has silently destroyed its own usefulness for backtesting: the
number that existed at 08:30 on release day is not the number in the table after
two revisions. Backtesting against revised data is lookahead bias, quietly.

`events` therefore stores:

```
(event_id, scheduled_ts, observed_ts, actual, forecast, previous, revision_seq)
```

— what was known, and *when* it was known.

This costs almost nothing in storage and is plausibly the single most valuable
property of the platform, because most free sources get it wrong.

---

## 4. Data sources

Two orthogonal axes now matter. **Ingestion pattern** (how the data arrives)
determines the engineering challenge; **group** (§3.3) determines the schema it
lands in.

| Ingestion pattern | Mechanism | Examples | Engineering challenge |
|---|---|---|---|
| REST APIs | Batch (scheduled pulls) | Binance/Kraken OHLCV, Polygon, yfinance, FRED | Pagination, rate limits, retries, incremental loading, backfills |
| WebSockets | Streaming (pushed events) | Binance/Kraken live trade feeds, L2 book | Connection management, reconnection, buffering, micro-batching |
| Files | Static / semi-static | Symbol reference CSVs, exchange calendars, macro downloads | Schema drift, encoding, slowly changing dimensions |
| Scheduled releases | Batch, but **revised** | Economic calendar, earnings | Bitemporality, revision tracking, late arrival |

The batch side already exists in `qde`. **Streaming remains the biggest genuine
skill gap.** The fourth row is new, and its challenge is not mechanical but
modelling — getting the temporal model right (§3.4) rather than getting the HTTP
right.

---

## 5. Target architecture

```
  REST APIs    WebSockets    Files/CSV    Scheduled releases
      │             │            │              │
      └─────────────┴─────┬──────┴──────────────┘
                          ▼
              ┌───────────────────────┐
              │   SOURCE REGISTRY     │  ← one SourceSpec per source
              │   (the little book)   │     config · DQ contract · catalogue
              └───────────┬───────────┘
                          ▼
              BaseIngestor subclasses (qde)
              retries · pagination · validation        ┌─────────────┐
                          │                            │   Dagster   │
                          ▼                            │ one asset   │
              Parquet lake on Cloudflare R2            │ per GROUP,  │
              bronze → silver → gold                   │ partitioned │
              partitioned by group / source / date     │ by date     │
                          │                            └─────────────┘
                          ▼
              dbt + DuckDB transformations
              tested, documented SQL models
                          │
          ┌───────────────┴────────────────┐
          ▼                                ▼
  PUBLIC PARQUET LAKE              Catalogue service
  (analysts point their            (FastAPI: what exists,
   own DuckDB at R2)                what's fresh, what's
                                    the schema, what's the
                                    licence) + Streamlit
```

**The medallion pattern** remains the storage backbone:

- **Bronze** — raw API responses, exactly as received, partitioned by
  group/source/date. Never modified. The replay log: if a transformation bug is
  found, rebuild everything downstream from bronze.
- **Silver** — cleaned, deduplicated, typed, schema-enforced. Conforms to the
  group's shared schema. One row per candle/trade/observation.
- **Gold** — analytics-ready: resampled OHLCV, rolling volatility, spreads,
  microstructure features, joined macro context.

### 5.1 Serving model: publish files, not queries

**The most consequential decision in the project**, and a reversal of the
original Phase 8.

**Rejected — serve queries.** FastAPI + DuckDB executing analyst queries means
paying compute per query. Cost scales with users, and one analyst running a
full-table scan in a loop generates a real bill. This is the model that bankrupts
self-funded platforms.

**Chosen — serve files.** A hive-partitioned Parquet lake on R2 with a public
catalogue. Analysts point their *own* DuckDB at it:

```sql
SELECT *
FROM read_parquet('https://data.<domain>/bars/source=binance/date=2026-*/*.parquet');
```

Their machine does the compute. Partition pruning and Parquet column pushdown
mean they transfer only what they touch. **R2's zero egress fee** — already the
reason R2 was chosen — is the single property that makes this economically
viable; the same design on S3 would generate a real bill from a handful of users.

**Therefore the API changes purpose.** It is no longer a query engine. It becomes
a thin **catalogue and metadata service**: what datasets exist, what schema, what
freshness, what DQ statistics, what licence. The `dim_sources` little-book table
*is* that catalogue.

The catalogue is the product. A small demo query endpoint may still exist for
convenience, rate-limited — but it is a demo, not the interface.

### 5.2 Cost control

- `bars`, `series`, `events` are **cheap**. Decades of daily equity bars are a few
  GB in Parquet; every FRED series ever published is smaller. Storage here costs
  cents per month. This is what makes "house everything" affordable.
- `microstructure` is the only expensive group and will outweigh everything else
  combined by orders of magnitude. It is **strictly scoped**: a small instrument
  set, a rolling hot-retention window (~90 days **[open]**), after which raw L2 is
  aggregated into microstructure features and the raw is dropped.
- The "house everything" ambition applies to the cheap groups only. This asymmetry
  is deliberate and should be stated in the README.

---

## 6. Licensing: the constraint that could kill this

Most financial data **cannot legally be redistributed**. This is a hard limit on
the platform ambition and must be settled *before* public publishing — building
ingestion for data that cannot be published would be wasted effort at platform
scope.

Rough picture, **to be verified source by source**:

| Source | Redistributable? |
|---|---|
| Exchange-native public data (Binance, Kraken historical) | Generally yes |
| Government / central bank macro (FRED and similar) | Generally yes |
| yfinance | **No** — scrapes Yahoo; Yahoo's terms prohibit redistribution |
| Polygon and commercial vendors generally | **No** — licence forbids redistribution |
| Commercial economic-calendar aggregators | Usually **no** |

### 6.1 The resulting shape of the product

Two halves, cleanly separated:

1. **The open lake** — data I am permitted to publish. Exchange data, government
   macro, anything openly licensed. Free, hosted, queryable directly from R2.
2. **The open-source ingestion code** — for licensed sources I publish the
   *ingestor*, not the data. An analyst supplies their own API key and pulls
   Polygon (or whatever) into the **same group schema** as everything else.

This is more defensible than it first looks. The schema unification is the part
nobody wants to do, and it is given away in *both* halves. Every `SourceSpec`
therefore carries `redistributable: bool`, and the publishing job refuses to
write anything marked `False` into the public lake.

---

## 7. The phases

Reordered from the original. Two structural changes:

- **The registry now precedes orchestration.** Building Dagster assets over
  hardcoded per-source jobs and *then* refactoring to a registry means writing
  them twice.
- **A licensing audit is inserted early** as a gate on public publishing.

### Phase 0 — Harden the foundation (1–2 weeks) — *unchanged*

Before adding anything new, make the existing code production-grade.

| Task | Tool | Why |
|---|---|---|
| Unit + integration tests | **pytest** | Test pagination, retry behaviour (mock 429s), Parquet round-trips. Interviewers *will* ask "how do you test this?" |
| Static typing | **mypy** | Catches bugs before runtime; signals maturity. |
| Linting + formatting | **ruff** | Replaces flake8 + black + isort. One config in `pyproject.toml`. |
| Structured logging | **structlog** | Leveled, timestamped, JSON-capable logs. Essential once things run unattended. |
| Config management | **pydantic-settings** | Typed settings from env. Also the foundation the registry builds on. |

**Deliverable:** `pytest` green in CI, `ruff` and `mypy` clean, all loaders logging structured events.

### Phase 1 — Lakehouse storage on Cloudflare R2 (1 week) — *unchanged*

Move the lake from local disk to object storage. The decoupling move.

| Task | Tool | Why |
|---|---|---|
| Object storage | **Cloudflare R2** | S3-compatible, **zero egress fees** — now load-bearing for the whole serving model (§5.1), not just a cost saving. |
| S3-compatible access | **boto3** / **fsspec + s3fs** / DuckDB `httpfs` | DuckDB queries `s3://bucket/path/*.parquet` directly. |
| Partitioning | Hive-style paths | `bronze/group=bars/source=binance/symbol=BTCUSDT/date=2026-07-10/part-0.parquet` — note **group is now the outermost partition key.** |

**Deliverable:** all loaders write to R2; DuckDB queries remotely; partitioning scheme documented.

### Phase 2 — Licensing audit (3–5 days) — **NEW, gating**

Cheap to do, catastrophic to skip. Determines which sources can exist in the
public lake versus code-only (§6).

**Deliverable:** every current and planned source classified; `redistributable`
and `license_note` fields populated; the two-halves product shape documented in
the README.

### Phase 3 — Group schemas (1 week) — **NEW**

Define the shared schema for `bars`, `series`, `events`, `microstructure`
(§3.3). Get the bitemporal model right for `events` (§3.4) — this is a
one-way-ish door, since retrofitting revision history onto an already-populated
table means re-fetching everything.

**Deliverable:** four documented schemas in `docs/schemas/`; an ADR explaining
group-by-shape over group-by-asset-class.

### Phase 4 — Registry + `BaseIngestor` (2 weeks) — **NEW**

Build the little book. Refactor Binance / Kraken / yfinance onto the pattern.
Keep the hand-written loaders in git history — building pagination and retries by
hand *first*, then abstracting them, is precisely the trajectory worth showing.

**Deliverable:** `SourceSpec` registry; `BaseIngestor` ABC; three existing sources
migrated with no behavior change; `dim_sources` generated from the registry.

### Phase 5 — Source expansion (3–4 weeks) — *was Phase 2, now much wider*

Every addition here should be a registry row plus `fetch_page` / `normalize`. If
it is not, the abstraction in Phase 4 was wrong — that is the test.

| Task | Tool | Group | Why |
|---|---|---|---|
| Unify exchange access | **ccxt** | `bars` | One library, 100+ exchanges. Replaces the hand-written crypto loaders. |
| Macro series | **FRED API** | `series` | Rates, inflation, employment. Openly redistributable. |
| Volatility complex | CBOE / vendor **[open]** | `series` | VIX, VVIX, term structure. |
| Economic calendar | **[open]** — source TBD | `events` | The bitemporal one. Licensing is the blocker, not the engineering. |
| Equities | yfinance (code-only) / **[open]** | `bars` | Corporate actions are the pressure point — see §11. |

**Deliverable:** four groups populated; adding a source demonstrably costs one
registry row.

### Phase 6 — Streaming and backfills (2 weeks) — *was Phase 2*

| Task | Tool | Why |
|---|---|---|
| Live streaming | **websockets** | Async client. Subscribe to Binance trade streams, buffer in memory, flush micro-batches to bronze every 30–60s. Reconnects, gap detection. Feeds `microstructure`. |
| Incremental loading | watermark pattern | Last-loaded timestamp **per registry entry**. Plus idempotent partition overwrites — *the* core batch pattern. |
| Backfills | CLI, group-level | `qde backfill --group bars --source binance --from 2020-01-01`. Uniform because the group schema is uniform. |

**Deliverable:** live trades into bronze; watermarked incremental loads; a
group-level backfill command that works identically across sources.

### Phase 7 — Orchestration with Dagster (1–2 weeks) — *was Phase 3*

Manual runs and cron don't scale past a couple of jobs.

| Consideration | Dagster | Airflow |
|---|---|---|
| Mental model | Software-defined **assets** ("this table exists and is fresh") | **Tasks** ("run this script at 6am") |
| Local dev | Excellent — `dagster dev`, instant UI | Heavy — scheduler, webserver, metadata DB |
| Job market | Growing fast | Still the most common keyword |
| Fit here | Natural — the pipeline *is* a graph of assets | Works, more boilerplate |

**Recommendation: Dagster.** Its asset model maps directly onto group ×
partition. Assets are generated **from the registry**, not hand-written per
source — a new source appears in the Dagster UI without touching orchestration
code. Being able to *compare* Dagster to Airflow intelligently in an interview is
worth more than using Airflow badly.

**Deliverable:** registry-driven assets with schedules, retry policies, freshness
checks.

### Phase 8 — Transformations with dbt (2 weeks) — *was Phase 4*

| Task | Tool | Why |
|---|---|---|
| SQL framework | **dbt-core + dbt-duckdb** | Version-controlled, tested, documented models with dependency resolution. Zero warehouse cost. |
| Staging | dbt `staging/` | One model per group×source: rename, cast, deduplicate → silver. |
| Marts | dbt `marts/` | Gold: `fct_ohlcv_1h`, `fct_daily_returns`, `dim_symbols`, `dim_sources`, rolling volatility, volume profiles, **macro-joined context**. This is where the quant knowledge shows. |
| Docs | `dbt docs generate` | Auto lineage graph + column docs, hostable as a static site. |

**Deliverable:** full bronze→silver→gold lineage; docs site on GitHub Pages;
Dagster triggering `dbt build` after ingestion.

### Phase 9 — Data quality (1 week, then ongoing) — *was Phase 5*

Interviewers probe this hardest, because it's what juniors skip.

| Task | Tool | Why |
|---|---|---|
| Schema validation at ingestion | **Pandera** | Typed DataFrame schemas — thresholds **read from the registry**, not hardcoded. Fails fast at the bronze boundary. Lighter than Great Expectations at this size, and *saying that* shows judgment. |
| Transformation tests | **dbt tests** | `not_null`, `unique`, `accepted_values`, plus custom SQL: no candle where `high < low`; no gaps > 2 intervals; **no `observed_ts` before `scheduled_ts`** in events. |
| Freshness | dbt source freshness / Dagster freshness policies | SLA per registry entry. |

**Deliverable:** every silver/gold model tested; a data quality policy in `docs/`;
at least one custom financial-domain test (OHLC coherence is the classic; the
bitemporal ordering test is the more interesting one).

### Phase 10 — Observability (1 week) — *was Phase 6*

| Task | Tool | Why |
|---|---|---|
| Pipeline health | Dagster UI + alerts | Run history, failure notifications (Discord webhook — free, demo-friendly). |
| Metrics over the *data* | Gold "meta" models | Row counts per partition per day, ingestion lag, API error rates. Monitoring the data, not just the jobs, is the senior-level distinction. |

**Deliverable:** failure alerts wired; a pipeline-health page in the dashboard.

### Phase 11 — CI/CD (3–4 days) — *was Phase 7*

| Task | What runs | Why |
|---|---|---|
| CI on every PR | `ruff` → `mypy` → `pytest` → `dbt build --target ci` on sample data | Nothing broken merges. DataOps in practice. |
| CD | On merge: build image, deploy catalogue service | Full loop from commit to running service. |
| Scheduled fallback | Cron-triggered Actions for light ingestion | Free stopgap before hosted Dagster. |

**Deliverable:** green CI badge; automated deploy.

### Phase 12 — Catalogue and publishing (2–3 weeks) — *was Phase 8, purpose changed*

The "users can actually use it" part — deliberately last, because it is only as
good as everything beneath it.

| Task | Tool | Why |
|---|---|---|
| Public lake | R2 public bucket + hive partitions | The primary interface. Analysts query it with their own DuckDB. Publishing job filters on `redistributable`. |
| Catalogue service | **FastAPI** | Datasets, schemas, freshness, DQ stats, licence — generated from the registry. **[open]** whether this needs to be a live service at all, or a static JSON artifact emitted at build time (cheaper, probably sufficient). |
| Packaging | **Docker + docker-compose** | `docker compose up` brings up catalogue + Dagster locally. |
| Hosting | Render / Fly.io / HF Spaces | Free tier. Compute is tiny because analysts bring their own. |
| Dashboard | **Streamlit** | Public page: browse the catalogue, preview a dataset, see freshness. What non-technical people (and recruiters) actually click. |

**Deliverable:** a live URL where anyone can discover the data and copy a working
DuckDB query against it.

---

## 8. What we deliberately skip — and say why in the README

| Tool / approach | Why it's skipped (the interview answer) |
|---|---|
| **Spark** | Volume is gigabytes, not terabytes. DuckDB processes this on one machine faster than a Spark cluster spins up. Knowing when *not* to use Spark is the skill. Note the irony: the little-book pattern comes *from* Spark-land, but the pattern is about variety, not volume — and variety is engine-agnostic. |
| **Kafka** | One producer, one consumer, replayable sources. A WebSocket buffer + Parquet micro-batches gives the same guarantees at 1% of the operational burden. Revisit if multiple independent consumers need the live stream. |
| **Kubernetes** | One small container. docker-compose is honest; K8s here would be cosplay. |
| **Snowflake / BigQuery** | Cost. DuckDB + Parquet on R2 is a genuine lakehouse and arguably the more interesting architecture to discuss. |
| **Serving compute to users** | *(New.)* Query-serving costs scale with users and one bad query can generate a real bill. Publishing files on zero-egress storage pushes compute to the client and makes marginal cost per user ~zero. This is an economics decision, not a technical one — and being able to say that is the point. |

This table is itself a portfolio asset: it demonstrates architectural judgment,
the rarest junior-level trait.

---

## 9. Suggested order and rough timeline

Phases 0→12 in order. Roughly 4–6 months at part-time pace — longer than the
original estimate, because the scope genuinely widened. Each phase merges to main
working and documented before the next begins; the repo should *always* be
demoable.

**Timeline is deliberately soft.** I am reading and taking coursework alongside
this, and expect to revise the plan as I encounter better patterns. Locking dates
to a plan I know is incomplete would be false precision.

---

## 10. Interview talking points this project earns

- *"Walk me through your architecture"* → the diagram, the medallion story,
  decoupling via Parquet contracts, and the registry as the single source of truth.
- *"How do you handle many heterogeneous sources?"* → the little-book pattern:
  group by shape, one shared schema per group, one job per group, a registry that
  is simultaneously config, DQ contract, and public catalogue.
- *"How do you handle failures?"* → HTTP-level retries with backoff, Dagster retry
  policies at job level, idempotent partition overwrites at data level.
- *"How do you know your data is correct?"* → Pandera at the boundary with
  thresholds from the registry, dbt tests in transformation, freshness SLAs, OHLC
  coherence, bitemporal ordering tests.
- *"Tell me about a subtle bug you designed out"* → revision handling in economic
  data. Storing only the current value of a release is silent lookahead bias in any
  backtest that uses it. Bitemporal storage fixes it for free.
- *"Why DuckDB and not X?"* → single-node scale, open formats, two-way door.
- *"How does this stay affordable?"* → zero-egress storage plus publish-don't-serve.
  Marginal cost per user approaches zero.
- *"What would you change at 100x?"* → partitioned reads are already in place; swap
  DuckDB for ClickHouse/Trino, promote the WebSocket buffer to Kafka, move ingestion
  to autoscaling workers. The architecture survives; only components swap. That's
  the decoupling payoff.

---

## 11. Open questions

Tracked explicitly, because the honest answer to most of these is "I'll know more
after the next chapter."

- **[open] Group taxonomy.** Four groups is the current bet. Reference /
  fundamentals data (sector, market cap, index membership) doesn't obviously fit
  any of them and may need a fifth — likely a slowly-changing-dimension shape.
- **[open] Does `bars` survive equities?** Corporate actions, splits, and dividends
  may force equities out of the shared bar schema. Adjusted-vs-raw close is the
  pressure point. If it breaks, that's a fifth group, not a wider schema.
- **[open] Retention policy for `microstructure`.** 90 days hot is a guess.
- **[open] Catalogue: live service or static artifact?** Static JSON emitted from
  the registry at build time is cheaper and probably sufficient.
- **[open] Symbol normalization across venues.** Almost certainly needs its own
  mapping table. A well-known source of quiet, expensive bugs.
- **[open] Does medallion survive the group pattern?** Groups may make
  bronze/silver/gold partially redundant, or the two may compose cleanly
  (`group` as the outer partition, medallion as the inner). Currently assuming the
  latter.
- **[open] Economic calendar source.** Engineering is easy; licensing is the blocker.

---

## 12. Repo structure (end state)

```
quant-data-platform/
├── src/qde/
│   ├── registry/         # SourceSpec definitions — the little book
│   ├── schemas/          # shared schema per group (bars, series, events, micro)
│   ├── ingest/           # BaseIngestor + per-source fetch_page/normalize
│   ├── storage/          # R2 + Parquet writers, partitioning
│   ├── publish/          # public-lake publisher (filters on redistributable)
│   └── cli.py            # backfill, run commands
├── orchestration/        # Dagster — assets GENERATED from the registry
├── transform/            # dbt project (staging → marts)
├── catalogue/            # FastAPI catalogue service (or static generator)
├── dashboard/            # Streamlit app
├── tests/                # pytest suites
├── docs/
│   ├── schemas/          # the four group schemas
│   ├── adr/              # architecture decision records
│   └── licensing.md      # per-source redistribution audit
├── .github/workflows/
├── docker-compose.yml
└── pyproject.toml
```

A **monorepo with clear internal boundaries**, not microservices. At this scale
that's the honest and reviewable choice — the boundaries (folders + storage
contracts) are what matter, not deployment separation.