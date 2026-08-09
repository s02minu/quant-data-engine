-- Gold: per-observation features for every scalar series, one row per
-- (source, series_id, metric, date).
--
-- The macro/vol/positioning inputs Model 1 reads (docs/data-sources.md 2): the
-- level, its change since the prior observation, and how unusual that change is
-- versus recent history. Computed once here instead of by every consumer.
--
-- Frequency-agnostic by design: the series group spans daily rates, weekly claims,
-- monthly CPI, quarterly GDP. All windows are therefore in *observations*, not
-- calendar time -- `change` is observation-over-observation, and the z-score is
-- over the trailing 12 observations. This mirrors the bars mart's choice to leave
-- volatility unannualized: don't fake a calendar normalization the mixed grain
-- can't support -- consumers who need a per-annum view apply their own factor.
{{ config(
    materialized='external',
    location=var('lake_root') ~ '/gold/group=series/mart=fct_series_features/data.parquet',
    format='parquet'
) }}

with base as (
    select * from {{ ref('stg_series') }}
),

changes as (
    select
        *,
        lag(value) over w as prev_value,
        value - lag(value) over w as change,
        (value - lag(value) over w) / nullif(lag(value) over w, 0) as change_pct
    from base
    window w as (partition by source, series_id, metric order by date)
)

select
    source,
    series_id,
    metric,
    date,
    value,
    prev_value,
    change,
    change_pct,
    -- Standardize the current change against the distribution of the *preceding*
    -- 12 changes (window excludes the current row, so a point never inflates or
    -- deflates its own z). A |z| well above ~2 flags an unusually large move.
    (change - avg(change) over w_prior)
        / nullif(stddev_samp(change) over w_prior, 0) as change_z_12obs
from changes
window w_prior as (
    partition by source, series_id, metric order by date
    rows between 12 preceding and 1 preceding
)
