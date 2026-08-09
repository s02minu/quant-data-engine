-- Grain: the revisions mart collapses each release to exactly one row, so event_id
-- must be unique. Fails if any event_id appears more than once.
select
    event_id,
    count(*) as n
from {{ ref('fct_events_revisions') }}
group by 1
having count(*) > 1
