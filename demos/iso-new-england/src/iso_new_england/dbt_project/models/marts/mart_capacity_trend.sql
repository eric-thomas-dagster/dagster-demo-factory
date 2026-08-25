-- Trailing 3-day average peak load, alongside the day's own peak -- the
-- kind of trend line a platform-status view would chart.
select
    event_date,
    peak_reading_mw,
    round(avg(peak_reading_mw) over (
        order by event_date
        rows between 2 preceding and current row
    ), 2) as trailing_3day_avg_peak_mw
from {{ ref('mart_daily_operations_summary') }}
order by event_date
