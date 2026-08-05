# `series` schema

A `series` is a **scalar time series**: one numeric value per timestamp, per
series. It is the shape most non-price data takes — macro indicators, the
volatility complex, interest rates, positioning, crypto funding/OI. Tiny in volume
and cheap to house, which is what makes "house everything" affordable for
everything that isn't microstructure.

Every source writing to this group conforms to the columns below, so a consumer
joins VIX, CPI, the 10-year yield, and a perp funding rate the same way.

## Partition layout

One **mutable file per series**, keyed by the things you filter on. No `date=`
partition — a series is one row per period, so date-partitioning would spawn
one-row files (the small-files problem). `date` is a column inside the file. This
mirrors the `bars` decision exactly; only the payload differs.

```
bronze/group=series/source=<source>/series_id=<series_id>/series.parquet
```

- `source` — the origin, e.g. `fred`, `cboe`, `cftc`, `binance`. Partition key.
- `series_id` — the source-native series identifier, e.g. `CPIAUCSL` (FRED),
  `VIX` (CBOE), `BTCUSDT` (a crypto funding series). Partition key.

A source with many series (FRED) gets one file per series it is registered to pull;
the curated set comes from the registry, not "all of FRED".

## Column contract (bronze)

| Column | Type | Notes |
|---|---|---|
| `date` | timestamp, UTC | The observation period's timestamp (period **start**, UTC midnight for daily/lower frequencies). The row key. |
| `value` | float64 | The observation. Missing observations are `NaN` (FRED sends `.` for missing — coerced to `NaN`, the row is kept so gaps are visible). |

That is the whole bronze payload: `(date, value)`. Everything constant about the
series — its units, frequency, title, seasonal-adjustment, licence — is **registry
/ catalogue metadata**, not repeated on every row (see *Metadata* below).

### Optional metric dimension

Some "series-shaped" sources emit **several scalars per period for one symbol** —
e.g. a crypto perp has funding rate, open interest, and mark price all keyed by
`(symbol, time)`. Rather than one file per metric, these use a `metric` partition:

```
bronze/group=series/source=binance/series_id=BTCUSDT/metric=funding_rate/series.parquet
bronze/group=series/source=binance/series_id=BTCUSDT/metric=open_interest/series.parquet
```

`metric` is omitted for single-value sources (FRED, CBOE), where `series_id` alone
identifies the scalar.

## Vintages — the bitemporal extension (ALFRED)

Macro data gets **revised**: the GDP figure known on release day is not the figure
in the table after two revisions. Backtesting against the revised value is silent
lookahead bias. The default `series` file stores the **latest** value per date —
correct for current analysis, wrong for point-in-time backtests.

For sources that expose revision history (FRED's archival API, **ALFRED**), the
series carries two extra columns, and the row key becomes `(date, realtime_start)`:

| Column | Type | Notes |
|---|---|---|
| `realtime_start` | date | First day this value was the published value for `date`. |
| `realtime_end` | date | Last day it was current (`9999-12-31` while still current). |

A vintaged series lives beside the latest one under a `vintaged=true` marker so the
two don't mix:

```
bronze/group=series/source=fred/series_id=GDPC1/vintaged=true/series.parquet
```

To reconstruct "what was known on date D", filter `realtime_start <= D < realtime_end`.
Storing this costs almost nothing (macro series are tiny) and is plausibly the
single most valuable property of the macro half of the platform. The plain latest
series is the default; the vintaged variant is opt-in per registry series, because
most consumers want the current value and only backtests want the vintages.

## Metadata (registry / catalogue, not per row)

Constant-per-series facts live on the `SourceSpec` (and flow into `dim_series` /
the catalogue), so they are defined once and cannot drift from the data:

- `frequency` — `D` / `W` / `M` / `Q` / `A` (or irregular). Drives freshness SLAs.
- `units` — e.g. "Percent", "Index 1982-84=100", "Billions of Dollars".
- `title` / `description` — human label.
- `seasonal_adjustment` — SA / NSA (macro).
- `redistributable` + `license_note` — **per series** for FRED (a government series
  publishes; an ICE/S&P series on the same API stays code-only). See
  [`../data-sources.md`](../data-sources.md) §4.

## How current sources map

| Source | `series_id` | `value` | Vintaged? | Redistributable |
|---|---|---|---|---|
| FRED (gov series) | FRED code (`CPIAUCSL`, `DGS10`, `UNRATE`) | the observation | via ALFRED | yes |
| FRED (3rd-party) | FRED code (ICE credit, Case-Shiller) | the observation | via ALFRED | **no** (code-only) |
| CBOE | `VIX`, `VVIX`, `SKEW` | EOD index level | no | yes (EOD levels) |
| CFTC COT | future code + `metric` (net position, long, short) | contracts | no | yes |
| Binance/Bybit perp | symbol + `metric` (funding_rate, open_interest) | the metric | no | yes |

## Data-quality notes (feed Phase 9)

- **No duplicate `(date[, realtime_start])`** — the row key must be unique.
- **Frequency continuity** — gaps beyond the declared `frequency` are flagged
  (a monthly series missing a month), respecting that macro releases are scheduled.
- **`NaN` policy** — a `NaN` value is a legitimate "not published" marker, not a
  defect; a *missing row* where the calendar expected one is the defect.
- **Monotonic `realtime_start`** per `date` for vintaged series; `realtime_start`
  never precedes the observation being publishable.
