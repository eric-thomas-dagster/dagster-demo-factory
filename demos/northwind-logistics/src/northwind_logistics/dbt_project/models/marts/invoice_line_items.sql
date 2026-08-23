{{
    config(
        unique_key=['shipment_id'],
    )
}}

-- Left-joined to carrier_rate_validated: if a carrier's rate data is missing
-- for this partition, the affected lines get a null rate rather than a
-- silently wrong one. carrier_rate_arrival is what stops this from ever
-- reaching margin_by_lane_customer -- see that test for why.
select
    s.shipment_id,
    s.event_date as invoice_date,
    s.customer_id,
    s.carrier,
    s.lane_code,
    s.weight_lbs,
    r.rate_amount_usd,
    r.fuel_surcharge_pct,
    round(s.weight_lbs / 100.0 * r.rate_amount_usd * (1 + r.fuel_surcharge_pct), 2) as line_amount_usd
from {{ ref('shipment_events_clean') }} s
left join {{ ref('carrier_rate_validated') }} r
    on s.carrier = r.carrier
    and s.lane_code = r.lane_code
    and s.event_date = r.rate_date
where s.event_date >= '{{ var("min_date") }}'
  and s.event_date < '{{ var("max_date") }}'
