-- OHLC coherence (ROADMAP 9's custom financial test): a bar is malformed if the
-- high is not the max or the low is not the min of the four prices. The test fails
-- if it returns any row.
--
-- Tolerance: a *relative* epsilon (1e-6 x close), because yfinance's dividend
-- adjustment introduces floating-point noise (~1e-16 relative) that makes an
-- adjusted `close` differ from the high/low it should equal by a machine-epsilon
-- amount -- economically coherent, but tripping a strict `<`/`>`. A real defect (a
-- bad print, a decimal shift) is off by cents or more (>= ~1e-4 relative), orders
-- of magnitude above this floor, so the threshold separates noise from defects
-- cleanly.
{% set tol = "1e-6 * close" %}

select
    source,
    symbol,
    date,
    open,
    high,
    low,
    close
from {{ ref('fct_bars_daily') }}
where high < low - ({{ tol }})
   or high < open - ({{ tol }})
   or high < close - ({{ tol }})
   or low > open + ({{ tol }})
   or low > close + ({{ tol }})
