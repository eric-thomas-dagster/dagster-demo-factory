{{ config(unique_key='event_date') }}
select
    shipment_id,
    cast(event_date as date) as event_date,
    carrier,
    lane,
    origin,
    destination,
    customer_id,
    weight_lbs,
    miles,
    status,
    cast(shipped_at as timestamp) as shipped_at
from {{ source('raw', 'shipment_events_raw') }}
where event_date >= '{{ var("min_date") }}'
  and event_date <= '{{ var("max_date") }}'
