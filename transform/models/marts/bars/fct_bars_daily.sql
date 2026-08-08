-- Gold: analytics-ready daily bar features, one row per (source, symbol, date).
--
-- Materialized as a Parquet FILE in the lake (dbt-duckdb `external`), so it
-- publishes to R2 and clients query it with their own DuckDB. These are Model-2's
-- ATR/volatility inputs (docs/data-sources.md 2): returns, realized vol, ATR, and
-- a volume anomaly score -- computed once here instead of by every consumer.
--
-- Volatility is left as *daily* realized (stddev of log returns), not annualized:
-- the slice spans crypto (365 trading days) and ETFs (~252), so a single
-- annualization factor would be wrong for some. Consumers annualize with their own
-- calendar factor.
{{ config(
    materialized='external',
    location=var('lake_root') ~ '/gold/group=bars/mart=fct_bars_daily/data.parquet',
    format='parquet'
) }}

with base as (
    select *
    from {{ ref('stg_bars') }}
    where interval = '1d'
),

returns as (
    select
        *,
        close / lag(close) over w - 1 as ret_simple,
        ln(close / lag(close) over w) as ret_log,
        -- True range: the classic max of today's range and the gaps to the prior
        -- close. The base of ATR.
        greatest(
            high - low,
            abs(high - lag(close) over w),
            abs(low - lag(close) over w)
        ) as true_range
    from base
    window w as (partition by source, symbol order by date)
)

select
    source,
    symbol,
    interval,
    date,
    open,
    high,
    low,
    close,
    volume,
    ret_simple,
    ret_log,
    true_range,
    avg(true_range) over w14 as atr_14,
    stddev_samp(ret_log) over w20 as realized_vol_20d,
    stddev_samp(ret_log) over w30 as realized_vol_30d,
    (volume - avg(volume) over w30)
        / nullif(stddev_samp(volume) over w30, 0) as volume_z_30d
from returns
window
    w14 as (
        partition by source, symbol order by date
        rows between 13 preceding and current row
    ),
    w20 as (
        partition by source, symbol order by date
        rows between 19 preceding and current row
    ),
    w30 as (
        partition by source, symbol order by date
        rows between 29 preceding and current row
    )
