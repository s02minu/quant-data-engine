# Group schemas

The platform groups sources by **physical shape and access pattern**, not by asset
class (ROADMAP §3.3). Shape is what determines schema, partitioning, and storage
cost — so grouping by it is what lets a new instrument type be a registry row
instead of a new module.

Each group has one **shared schema**: every source that writes to a group conforms
to the same columns and partition layout, which is what makes "one job per group"
and uniform backfills possible.

## The four groups

| Group | Shape | Grain | Volume | Layout | Status |
|---|---|---|---|---|---|
| [`bars`](bars.md) | OHLCV time series | one row / period / symbol | Moderate | one file per series | **implemented** |
| [`series`](series.md) | `(date, value)` scalar series | one row / period / series | Tiny | one file per series | **this doc** |
| [`events`](events.md) | Scheduled, sparse, **bitemporal** | one row / release / revision | Tiny | one file per calendar | **this doc** |
| [`microstructure`](microstructure.md) | Tick / L2 order book | one row / message | **Enormous** | date-partitioned part files | **implemented** |

## Why grouping by shape (the decision)

The instinct is to group by asset class — crypto / equities / macro / volatility.
That is wrong, because it scatters one physical shape across many modules and forces
every consumer to special-case each asset class. Grouping by shape instead means:

- **VIX is not a special case.** It is one `series` row pointing at CBOE, sharing
  the schema of every FRED macro series and every crypto funding rate. New
  instrument types stop being new modules.
- **Backfills are uniform.** `backfill(group="series", source="fred", from=...)`
  works identically for every source in the group, because they write the same
  schema against the same partition key.
- **Cost is legible.** Three of the four groups (`bars`, `series`, `events`) are
  cheap — decades of daily bars or every FRED series ever published are a few GB in
  Parquet. Only `microstructure` is expensive, and it is strictly scoped. The
  "house everything" ambition applies to the cheap groups; the asymmetry is
  deliberate (ROADMAP §5.2).

## Shared conventions across all groups

- **Medallion + group compose.** `group` is the outermost partition key; the
  medallion layer (`bronze` / `silver` / `gold`) is outside it in the path. Bronze
  is raw-as-received and never modified; everything downstream rebuilds from it.
- **Path shape.** `<layer>/group=<group>/source=<source>/…/<file>.parquet`, Hive
  style, so DuckDB reads the partition keys back as filterable columns.
- **Time is UTC.** Every timestamp column is UTC-aware. Raw prices/values are
  preserved as received (numeric strings coerced, not rounded).
- **Low-volume groups are one mutable file per series** (`bars`, `series`), with the
  period as a *column*, not a `date=` partition — a one-row-per-day series would
  otherwise spawn thousands of one-row files (the small-files problem).
  High-volume `microstructure` is the opposite: many immutable date-partitioned
  part files.
- **Identity that is constant per series lives in the registry, not per row.** A
  series' units, frequency, title, and licence are `SourceSpec`/catalogue
  metadata; the bronze file carries only what varies per observation.
