-- Typed, deduplicated interval telemetry, one row per reporting point per hour.
select
    reporting_point_id,
    cast(event_date as date) as event_date,
    cast(interval_ending as timestamp) as interval_ending,
    reading_mw,
    quality_flag,
    source_system
from {{ source('staged', 'staged_readings') }}
where event_date >= '{{ var("min_date") }}'
  and event_date <= '{{ var("max_date") }}'
