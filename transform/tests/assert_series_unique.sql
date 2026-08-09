-- Grain: exactly one row per (source, series_id, metric, date) in the silver series
-- view. metric is NULL for flat sources, so coalesce it to a sentinel before
-- grouping (NULLs would otherwise not group together). Fails if any key repeats.
select
    source,
    series_id,
    coalesce(metric, '_') as metric,
    date,
    count(*) as n
from {{ ref('stg_series') }}
group by 1, 2, 3, 4
having count(*) > 1
