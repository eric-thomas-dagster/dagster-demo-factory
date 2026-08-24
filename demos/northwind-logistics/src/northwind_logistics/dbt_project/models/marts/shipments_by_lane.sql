select
    event_date,
    lane,
    carrier,
    count(*) as shipment_count,
    sum(weight_lbs) as total_weight_lbs,
    sum(miles) as total_miles
from {{ ref('shipment_events_clean') }}
where event_date >= '{{ var("min_date") }}'
  and event_date <= '{{ var("max_date") }}'
group by 1, 2, 3
