## quant-data-engine

Financial data engine that pulls, cleans, stores, and serves market data as a queryable local database.

**Status:** Batch pipeline and streaming collector both operational.  
**Batch:** REST loaders, Parquet storage, incremental updates, DuckDB SQL, quality monitoring.  
**Streaming:** live trades, L2 order-book deltas, and top-of-book quotes captured to a partitioned bronze lake — with micro-batching, reconnection, gap detection, and containerized 24/7 running.

### Architecture

```mermaid
flowchart TD
    subgraph SRC["Sources"]
      REST["Binance · Kraken · yfinance<br/>REST APIs"]
      WS["Binance WebSocket<br/>trades · depth · book_ticker"]
    end

    REST -->|"batch pull · pagination · retries"| BL["Batch loaders<br/>qde.loaders"]
    WS -->|"async read · buffer · flush"| SC["Stream collector<br/>qde.stream"]

    BL --> BARS[("Parquet: OHLCV bars<br/>data/ohlcv")]
    SC -->|"micro-batches"| BRONZE[("Bronze lake<br/>group=microstructure<br/>kind / symbol / date")]

    BARS --> DUCK["DuckDB<br/>SQL on Parquet"]
    BRONZE --> DUCK
    DUCK --> QA["Quality checks<br/>+ Power BI dashboard"]

    BRONZE -.-> SILVER["Silver / Gold<br/>dbt · book reconstruction · features"]
    BARS -.-> SILVER
    SILVER -.-> R2[("Cloudflare R2<br/>public Parquet lake")]
    R2 -.-> SERVE["Catalogue · serve files<br/>analysts query with their own DuckDB"]

    classDef planned stroke-dasharray:5 5;
    class SILVER,R2,SERVE planned;
```

Solid = operational today. Dashed = planned phases (full plan and reasoning in [docs/ROADMAP.md](docs/ROADMAP.md)).

### What it does

- Loads OHLCV data from Binance (REST API), Yahoo Finance, and Kraken (REST API), with unified symbol mapping across sources.
- Cleans and standardizes every DataFrame: lowercase columns, UTC-aware index, canonical OHLCV order — regardless of source.
- Stores data locally as Parquet files with incremental updates — fetch once, refresh daily, never re-download history.
- Queries stored data instantly via DuckDB SQL or direct DataFrame load — no API calls, no internet required.
- Monitors data quality automatically with four checks (gaps, duplicates, nulls, price sanity), surfaced in a Power BI dashboard.
- Captures live Binance microstructure — trades, order-book deltas, and top-of-book quotes — over WebSockets into a hive-partitioned bronze lake, buffering and flushing micro-batches, reconnecting on drops, and recording gaps so a hole in the tape is never silent.

### Quickstart

```bash
git clone https://github.com/s02minu/quant-data-engine.git
cd quant-data-engine
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```
To run the tests, install with the dev extras instead: `pip install -e ".[dev]"`

This local install covers development, the batch loaders, the tests, and ad-hoc
collector runs. To run the collector continuously, use Docker instead — see
[Streaming collector](#streaming-collector-microstructure) below.

### Usage

**Fetch and store data**
```python
from qde.storage import save_ohlcv

save_ohlcv("BTCUSDT", source="binance", start="2015-01-01")
save_ohlcv("SPY", source="yfinance", start="2015-01-01")
```

**Query stored data with SQL**
```python
from qde.storage import query

df = query("SELECT date, close FROM BTCUSDT_binance_1d WHERE close > 60000")
```

**Update with only new data**
```python
from qde.storage import update_ohlcv

update_ohlcv("BTCUSDT", source="binance")
```

### Streaming collector (microstructure)

Captures live Binance data — trades, L2 order-book deltas, and top-of-book quotes —
into a hive-partitioned bronze Parquet lake. It buffers in memory and flushes
micro-batches on a fixed interval, reconnects with exponential backoff, and records
sequence gaps and session boundaries so downtime is always visible rather than silent.
Unlike the batch OHLCV data, streamed microstructure is un-backfillable — the design
prioritizes never losing what can never be re-fetched.

Run locally (Ctrl-C flushes buffered rows before exit):
```bash
python -m qde.stream
```

Configure via environment variables:
```bash
QDE_SYMBOLS=BTCUSDT,ETHUSDT QDE_KINDS=trades,depth,book_ticker python -m qde.stream
```

Run continuously in Docker — auto-restart on crash, graceful shutdown on stop,
data persisted to the host `./data`:
```bash
docker compose up --build -d
```

Captured data lands under a partitioned bronze layout:
```
data/bronze/group=microstructure/source=binance/kind=<kind>/symbol=<symbol>/date=<date>/part-*.parquet
```
Query it directly with DuckDB, reading partition keys as columns:
```python
import duckdb

duckdb.sql(
    "SELECT kind, symbol, count(*) FROM read_parquet('data/bronze/**/*.parquet', hive_partitioning=true) GROUP BY ALL"
)
```

### Project structure
```
src/qde/
├── __init__.py               # Package root
├── storage.py                # Save, load, update Parquet + DuckDB query
├── quality.py                # Data quality checks + summary
├── loaders/                  # Batch REST ingestion (OHLCV bars)
│   ├── __init__.py           # Unified load_ohlcv() with source routing
│   ├── http.py               # Retry helper with exponential backoff
│   ├── binance_loader.py     # Binance REST API, pagination, epoch → UTC
│   ├── yfinance_loader.py    # Yahoo Finance loader, MultiIndex handling
│   ├── kraken_loader.py      # Kraken REST API, cursor pagination
│   └── symbols.py            # Cross-source symbol mapping
└── stream/                   # WebSocket capture → bronze (microstructure)
    ├── config.py             # StreamConfig: symbols, kinds, flush window
    ├── collector.py          # Async connect, buffer, flush, reconnect
    ├── parsers.py            # Raw payload → flat bronze row
    ├── gaps.py               # Sequence-gap and session-boundary tracking
    ├── paths.py              # Hive-partitioned bronze path builder
    └── __main__.py           # `python -m qde.stream` entry point

Dockerfile, docker-compose.yml     # Containerized 24/7 collector
```

### Tech stack
| Tool | Role |
|------|------|
| pandas | Data manipulation and DataFrame standard |
| requests | Direct HTTP calls to Binance and Kraken REST APIs |
| yfinance | Yahoo Finance convenience wrapper |
| websockets | Async client for Binance live streams |
| asyncio | Concurrency for the streaming collector (read loop + flush + snapshot) |
| pyarrow | Parquet read/write engine |
| DuckDB | SQL queries directly on Parquet files |
| pandas-market-calendars | NYSE calendar for equity gap detection |
| pytest | Automated testing |
| Docker | Containerized 24/7 collector with restart policy |

### Tests
```bash
pytest
```
Covering loader contracts, symbol mapping, storage round-trips, and error handling for
the batch side; and config, path, parser, gap detection, and a mocked-socket reconnect
test for the streaming side. The streaming tests fake the WebSocket, so they run offline
and deterministically — including the disconnect-and-recover path that can't be triggered
against the live exchange.

### Data quality monitoring

Automated daily quality checks with a Power BI dashboard connected to pipeline output.

<img src="assets/dashboard_quality.png" width="600" alt="Data quality dashboard">


### Roadmap

This project evolves deliberately — every tool must solve a real problem
before it's added. Full plan with phases and reasoning: [docs/ROADMAP.md](docs/ROADMAP.md)

**Planned evolution:** local Parquet → Cloudflare R2 object storage →
Dagster orchestration → dbt transformations on DuckDB → FastAPI serving layer

**Deliberately deferred at current scale:** Spark (data fits on one machine —
DuckDB is faster here), Kafka (one producer, one consumer, replayable sources),
Kubernetes (a single container doesn't need an orchestrator). These get
revisited when the constraints that justify them actually appear.

### Limitations
- Batch loader tests hit live APIs (not yet mocked); streaming tests run offline.
- Single-user local storage only — no concurrent access. Object storage (R2) is a planned phase.
- Symbol mapping is manual — new symbols must be added to symbols.py.
- Kraken's public OHLC endpoint serves only ~720 recent candles per interval, regardless of start date — deep history requires its paid data service.
- Order-book reconstruction from depth deltas is deferred to a later transform; the collector captures raw deltas and periodic snapshots (bronze), not a rebuilt book.
- Latency (`received_at − event_time`) is subject to clock skew between machines; treat absolute values as approximate.

### License
MIT