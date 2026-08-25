-- One row per day: the headline operational summary across all reporting points.
select
    event_date,
    count(distinct reporting_point_id) as reporting_point_count,
    sum(interval_count) as total_intervals,
    round(avg(avg_reading_mw), 2) as avg_reading_mw,
    round(max(peak_reading_mw), 2) as peak_reading_mw,
    sum(estimated_interval_count) as estimated_interval_count,
    max(advisory_count) as advisory_count
from {{ ref('int_daily_rollup') }}
group by 1
