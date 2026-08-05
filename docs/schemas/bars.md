# `bars` schema

A `bars` series is **OHLCV** — open/high/low/close/volume per period, per symbol.
The classic price time series: crypto spot, equity daily, futures. Moderate
volume. **Implemented** (`qde.storage`, `qde.ingest`); documented here for
completeness.

## Partition layout

One **mutable file per series** — no `date=` partition, because a daily series is
one row per day and date-partitioning would spawn one-row files. `date` is a column
inside the file.

```
bronze/group=bars/source=<source>/symbol=<symbol>/interval=<interval>/bars.parquet
```

- `source` — `binance`, `kraken`, `yfinance`. Partition key.
- `symbol` — canonical symbol, e.g. `BTCUSDT`. Partition key.
- `interval` — bar size, e.g. `1d`. Partition key.

## Column contract (bronze)

| Column | Type | Notes |
|---|---|---|
| `date` | timestamp, UTC | Bar open time. The row key (index in the file). |
| `open` `high` `low` `close` | float64 | Coerced from the source's numeric strings. |
| `volume` | float64 | Base-asset volume. |

## Writes

`upsert_bars` merges an incoming frame into the series file, deduping by `date`
(last-write-wins), then writes via temp→rename (crash-safe). A repeated or
overlapping pull converges to one row per date — the idempotent partition overwrite.
Incremental loads advance from the series' own watermark (`bars_watermark`, the max
stored `date`), so no sidecar ledger can drift.

See [`series.md`](series.md) for the scalar-series cousin and the shared
one-file-per-series rationale.
