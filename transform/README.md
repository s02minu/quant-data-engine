# `transform/` — dbt (bronze → silver → gold)

The transformation layer (ROADMAP Phase 8): **dbt-core + dbt-duckdb** turning the
raw bronze lake into tested, documented **gold** marts. Zero warehouse cost — dbt
reads bronze Parquet and materializes gold Parquet in place, so the marts publish to
R2 and clients query them with their own DuckDB (serve files, not queries).

## Layout

```
models/
  staging/bars/stg_bars.sql     # silver: cleaned, typed bars (view over bronze)
  marts/bars/fct_bars_daily.sql # gold: returns, true range/ATR(14), realized vol, volume z
  marts/dim_sources.sql         # gold: the source catalogue (from the registry)
tests/                          # singular tests: OHLC coherence, key uniqueness, ATR >= 0
seeds/dim_sources_seed.csv      # registry projection (regenerated; see below)
```

**Materialization.** Staging is a **view** (bronze is already deduped/typed by the
ingestors, so silver is thin). Gold is **external** — dbt-duckdb writes a Parquet
file into the lake at `gold/group=bars/mart=fct_bars_daily/data.parquet` and
`gold/dim_sources/data.parquet`.

**Source path.** Models read bronze via `read_parquet` under the `lake_root` var
(default `../data`; the VPS passes `--vars 'lake_root: /data'`). No R2 creds needed —
dbt runs against the local/mounted bronze.

## Run it

From this directory (`DBT_PROFILES_DIR=.` points dbt at the in-repo `profiles.yml`):

```bash
# DuckDB's COPY won't create dirs; make them once (the nightly does this too)
mkdir -p ../data/gold/group=bars/mart=fct_bars_daily ../data/gold/dim_sources

# regenerate the catalogue seed from the current registry (only if it changed)
python -c "from qde.registry import dim_sources; dim_sources().to_csv('seeds/dim_sources_seed.csv', index=False)"

DBT_PROFILES_DIR=. dbt build   # run models + tests
DBT_PROFILES_DIR=. dbt docs generate && DBT_PROFILES_DIR=. dbt docs serve  # lineage site
```

Query the result the same way locally or over R2 — the views are registered in both
`qde.storage.query` (local) and `qde.lake.query` (R2):

```sql
SELECT * FROM fct_bars_daily WHERE symbol = 'BTCUSDT' ORDER BY date DESC LIMIT 5;
SELECT * FROM dim_sources;
```

## In the pipeline

`scripts/maintain.sh` runs `dbt build` between the bronze update and the sync, so
gold rebuilds nightly from fresh bronze and `qde.sync.publish_gold` ships it to R2.
The `transform` extra (`pip install .[transform]`) is baked into the VPS image.

## Scope

This is the **vertical slice**: `bars` end-to-end. `series`/`events` staging + marts
(macro-joined context, surprise) are the next repeat of the pattern. Microstructure
stays in `qde.analytics` (Python), not dbt.
