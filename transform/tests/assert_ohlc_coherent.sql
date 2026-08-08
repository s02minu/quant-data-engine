-- OHLC coherence (ROADMAP 9's custom financial test): a bar is malformed if the
-- high is not the max or the low is not the min of the four prices. The test fails
-- if it returns any row.
select
    source,
    symbol,
    date,
    open,
    high,
    low,
    close
from {{ ref('fct_bars_daily') }}
where high < low
   or high < open
   or high < close
   or low > open
   or low > close
