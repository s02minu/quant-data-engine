-- Silver: the cleaned bitemporal events (release calendar) view over bronze.
--
-- Bronze events are one row per (event_id, revision_seq) -- a release and each of
-- its revisions (docs/schemas/events.md). Staging is thin: read the calendar
-- files, type the columns, and derive `reference_date` (the period the figure
-- describes) from the event_id, whose format is `<series_id>:<YYYY-MM-DD>` -- the
-- reference period is not stored as its own bronze column, only encoded in the id.
{{ config(materialized='view') }}

with bronze as (
    select *
    from read_parquet(
        '{{ var("lake_root") }}/bronze/group=events/**/*.parquet',
        hive_partitioning = true
    )
)

select
    source,
    calendar,
    event_id,
    series_id,
    -- event_id is "<series_id>:<ref-date>"; series_id carries no colon, so the part
    -- after the ':' is the reference period.
    cast(split_part(event_id, ':', 2) as date) as reference_date,
    cast(scheduled_ts as timestamp) as scheduled_ts,
    cast(observed_ts as timestamp) as observed_ts,
    cast(actual as double) as actual,
    cast(forecast as double) as forecast,
    cast(previous as double) as previous,
    cast(revision_seq as integer) as revision_seq
from bronze
