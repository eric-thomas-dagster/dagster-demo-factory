-- Readings enriched with same-day advisory context. A reporting point's
-- interval count is a *row-count* signal, not a null-value one -- a partial
-- batch shows up as fewer rows, which is why `staged_readings_completeness`
-- is a blocking asset check rather than a dbt not-null test.
with readings as (
    select * from {{ ref('stg_readings') }}
),

advisory_counts as (
    select event_date, count(*) as advisory_count
    from {{ ref('stg_reference') }}
    group by 1
)

select
    r.reporting_point_id,
    r.event_date,
    r.interval_ending,
    r.reading_mw,
    r.quality_flag,
    coalesce(a.advisory_count, 0) as advisory_count
from readings r
left join advisory_counts a
    on r.event_date = a.event_date
