-- One row per reporting point per day: the grain every downstream mart aggregates from.
select
    event_date,
    reporting_point_id,
    count(*) as interval_count,
    avg(reading_mw) as avg_reading_mw,
    max(reading_mw) as peak_reading_mw,
    sum(case when quality_flag = 'estimated' then 1 else 0 end) as estimated_interval_count,
    max(advisory_count) as advisory_count
from {{ ref('int_readings_validated') }}
group by 1, 2
