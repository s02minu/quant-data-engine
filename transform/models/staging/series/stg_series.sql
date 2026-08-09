-- Silver: the cleaned scalar-series view over bronze.
--
-- Bronze series are already deduped (upsert last-write-wins) and typed by the
-- ingestors, so staging is thin. The one real job here is the **mixed partition
-- depth**: single-value sources (FRED, CBOE) sit at source/series_id/series.parquet
-- while multi-metric sources (CFTC COT, perps) add a metric= level
-- (docs/schemas/series.md). DuckDB rejects a single glob spanning both depths
-- ("Hive partition mismatch"), so read each depth separately and UNION them, with
-- the flat side carrying metric = NULL -- the same reconciliation qde.storage.query
-- and qde.lake do, done once here so every downstream series mart is depth-agnostic.
{{ config(materialized='view') }}

with flat as (
    -- source/series_id/series.parquet -- FRED, CBOE (no metric)
    select source, series_id, cast(null as varchar) as metric, date, value
    from read_parquet(
        '{{ var("lake_root") }}/bronze/group=series/*/*/series.parquet',
        hive_partitioning = true
    )
),

metric as (
    -- source/series_id/metric=/series.parquet -- CFTC COT, perp funding
    select source, series_id, metric, date, value
    from read_parquet(
        '{{ var("lake_root") }}/bronze/group=series/*/*/*/series.parquet',
        hive_partitioning = true
    )
),

unioned as (
    select * from flat
    union all
    select * from metric
)

select
    source,
    series_id,
    metric,
    cast(date as timestamp) as date,
    cast(value as double) as value
from unioned
