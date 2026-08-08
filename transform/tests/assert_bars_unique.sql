-- One row per (source, symbol, interval, date). Fails if any key appears twice --
-- the grain contract of the bars group (bronze upsert dedups, so this guards a
-- regression). Built-in singular test, no dbt_utils dependency.
select
    source,
    symbol,
    interval,
    date,
    count(*) as n
from {{ ref('stg_bars') }}
group by source, symbol, interval, date
having count(*) > 1
