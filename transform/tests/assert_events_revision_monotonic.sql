-- Bitemporal sanity restated at gold: a release's latest vintage can never be
-- known before its initial print, and the revision count is non-negative. Bronze's
-- run_events_checks already enforces observed_ts >= scheduled_ts row-wise; this
-- guards the aggregation itself. Fails if any release violates it.
select
    event_id
from {{ ref('fct_events_revisions') }}
where latest_observed_ts < initial_observed_ts
   or n_revisions < 0
