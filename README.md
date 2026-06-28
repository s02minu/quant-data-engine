## quant-data-engine

Financial data engine that pulls, cleans, stores, and serves market data as a queryable local database.

**Status:** Core pipeline operational — loaders, storage, incremental updates, SQL queries, quality monitoring.  
**In progress:** WebSocket trade collector, order book snapshots.

### Architecture

<img src="assets/diagram_architecture.png" width="600" alt="Architecture diagram">

### What it does

- Loads OHLCV data from Binance (REST API), Yahoo Finance, and Kraken (REST API), with unified symbol mapping across sources.
- Cleans and standardizes every DataFrame: lowercase columns, UTC-aware index, canonical OHLCV order — regardless of source.
- Stores data locally as Parquet files with incremental updates — fetch once, refresh daily, never re-download history.
- Queries stored data instantly via DuckDB SQL or direct DataFrame load — no API calls, no internet required.
- Monitors data quality automatically with four checks (gaps, duplicates, nulls, price sanity), surfaced in a Power BI dashboard.

### Quickstart

```bash
git clone https://github.com/s02minu/quant-data-engine.git
cd quant-data-engine
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```
To run the tests, install with the dev extras instead: `pip install -e ".[dev]"`

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

### Project structure
```
src/qde/
├── __init__.py               # Package root
├── storage.py                # Save, load, update Parquet + DuckDB query
├── quality.py                # Data quality checks + summary
└── loaders/
    ├── __init__.py           # Unified load_ohlcv() with source routing
    ├── http.py               # Retry helper with exponential backoff
    ├── binance_loader.py     # Binance REST API, pagination, epoch → UTC
    ├── yfinance_loader.py    # Yahoo Finance loader, MultiIndex handling
    ├── kraken_loader.py      # Kraken REST API, cursor pagination
    └── symbols.py            # Cross-source symbol mapping
```

### Tech stack
| Tool | Role |
|------|------|
| pandas | Data manipulation and DataFrame standard |
| requests | Direct HTTP calls to Binance and Kraken REST APIs |
| yfinance | Yahoo Finance convenience wrapper |
| pyarrow | Parquet read/write engine |
| DuckDB | SQL queries directly on Parquet files |
| pandas-market-calendars | NYSE calendar for equity gap detection |
| pytest | Automated testing |

### Tests
```bash
pytest
```
Tests covering loader contracts, symbol mapping, storage round-trips, and error handling.

### Data quality monitoring

Automated daily quality checks with a Power BI dashboard connected to pipeline output.

<img src="assets/dashboard_quality.png" width="600" alt="Data quality dashboard">

### Roadmap
- **WebSocket trade collector** — live tick-level trade streaming from Binance.
- **Order book snapshots** — periodic depth snapshots for microstructure analysis.

### Limitations
- Tests require internet access (API responses are not mocked).
- Single-user local storage only — no concurrent access.
- Symbol mapping is manual — new symbols must be added to symbols.py.
- Kraken's public OHLC endpoint serves only ~720 recent candles per interval, regardless of start date — deep history requires its paid data service.

### License
MIT