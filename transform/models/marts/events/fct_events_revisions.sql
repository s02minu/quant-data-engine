-- Gold: the revision profile of every economic release, one row per event_id.
--
-- This is the payoff of storing the calendar bitemporally (ROADMAP 3.4). Bronze
-- keeps one row per (event_id, revision_seq) -- the initial print and each later
-- vintage; this mart collapses that history into the summary a backtest actually
-- wants: what was *first* known, what it was *finally* revised to, and how far /
-- how long it moved. The gap between initial_value and latest_value is exactly the
-- lookahead bias a current-value-only calendar would silently bake in.
--
-- `forecast` is intentionally absent from the aggregation: it is always NULL in the
-- free calendar (the consensus is the proprietary, code-only column), so a surprise
-- (actual - forecast) mart is a code-only extension layered on top per user.
{{ config(
    materialized='external',
    location=var('lake_root') ~ '/gold/group=events/mart=fct_events_revisions/data.parquet',
    format='parquet'
) }}

with events as (
    select * from {{ ref('stg_events') }}
)

select
    source,
    calendar,
    series_id,
    event_id,
    reference_date,
    -- scheduled_ts is constant across an event's revisions (the release date).
    min(scheduled_ts) as scheduled_ts,
    -- Value as first published vs as latest known (arg_min/arg_max over the vintage
    -- order). revision_seq 0 is the initial print; the max is the latest vintage.
    arg_min(actual, revision_seq) as initial_value,
    arg_max(actual, revision_seq) as latest_value,
    min(observed_ts) as initial_observed_ts,
    max(observed_ts) as latest_observed_ts,
    -- Number of revisions after the initial print (0 = never revised).
    max(revision_seq) as n_revisions,
    -- How far the figure moved from first print to latest, absolute and relative.
    arg_max(actual, revision_seq) - arg_min(actual, revision_seq) as total_revision,
    (arg_max(actual, revision_seq) - arg_min(actual, revision_seq))
        / nullif(arg_min(actual, revision_seq), 0) as revision_pct,
    -- How long the number kept moving: initial release to latest known vintage.
    date_diff('day', min(observed_ts), max(observed_ts)) as days_to_latest
from events
group by source, calendar, series_id, event_id, reference_date
