-- ATR and true range are ranges, so they can never be negative. Fails on any
-- negative value (a window/logic regression would surface here).
select
    source,
    symbol,
    date,
    true_range,
    atr_14
from {{ ref('fct_bars_daily') }}
where true_range < 0
   or atr_14 < 0
