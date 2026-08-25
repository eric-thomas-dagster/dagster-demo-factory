-- Per-reporting-point reliability over the window: how many days it reported,
-- and what share of its intervals came through as "estimated" rather than "good".
select
    reporting_point_id,
    count(distinct event_date) as days_reporting,
    sum(interval_count) as total_intervals,
    sum(estimated_interval_count) as estimated_interval_count,
    round(1.0 * sum(estimated_interval_count) / nullif(sum(interval_count), 0), 4) as estimated_rate
from {{ ref('int_daily_rollup') }}
group by 1
