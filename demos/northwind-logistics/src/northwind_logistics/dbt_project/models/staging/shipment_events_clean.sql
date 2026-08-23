{{
    config(
        unique_key=['shipment_id'],
    )
}}

select
    shipment_id,
    event_date,
    carrier,
    lane_code,
    customer_id,
    weight_lbs,
    declared_value_usd
from {{ source('raw', 'shipment_events_raw') }}
where event_date >= '{{ var("min_date") }}'
  and event_date < '{{ var("max_date") }}'
