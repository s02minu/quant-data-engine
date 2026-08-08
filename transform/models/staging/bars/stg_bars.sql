-- Silver: the cleaned bars view over bronze.
--
-- Bronze bars are already deduped (upsert last-write-wins) and typed by the
-- ingestors, so staging is deliberately thin: read the hive-partitioned bronze
-- Parquet directly (read_parquet, not a dbt source, so the lake_root var renders
-- reliably), surface the partition keys as columns, and pin explicit types. One
-- row per (source, symbol, interval, date).
{{ config(materialized='view') }}

with bronze as (
    select *
    from read_parquet(
        '{{ var("lake_root") }}/bronze/group=bars/**/*.parquet',
        hive_partitioning = true
    )
)

select
    source,
    symbol,
    interval,
    cast(date as timestamp) as date,
    cast(open as double) as open,
    cast(high as double) as high,
    cast(low as double) as low,
    cast(close as double) as close,
    cast(volume as double) as volume
from bronze
