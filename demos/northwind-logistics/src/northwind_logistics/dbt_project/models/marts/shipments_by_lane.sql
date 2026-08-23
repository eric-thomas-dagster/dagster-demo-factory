{{
    config(
        unique_key=['ship_date', 'lane_code', 'carrier'],
    )
}}

select
    event_date as ship_date,
    lane_code,
    carrier,
    count(*) as shipment_count,
    sum(weight_lbs) as total_weight_lbs,
    sum(declared_value_usd) as total_declared_value_usd
from {{ ref('shipment_events_clean') }}
where event_date >= '{{ var("min_date") }}'
  and event_date < '{{ var("max_date") }}'
group by 1, 2, 3
