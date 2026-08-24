-- Recomputes automatically when either upstream updates (see
-- AutomationCondition.eager() in defs/transformation/marts/defs.yaml) --
-- this is the asset that has to catch up on its own once a healed carrier
-- partition is rematerialized, with nobody clicking it.
{{ config(unique_key='event_date') }}

select
    sbl.event_date,
    sbl.lane,
    sbl.carrier,
    sbl.total_miles,
    cr.rate_per_mile,
    cr.fuel_surcharge_pct,
    sbl.total_miles * cr.rate_per_mile * (1 + cr.fuel_surcharge_pct) as allocated_cost
from {{ ref('shipments_by_lane') }} sbl
inner join {{ ref('stg_carrier_rates') }} cr
    on sbl.event_date = cr.event_date
    and sbl.lane = cr.lane
    and sbl.carrier = cr.carrier
where sbl.event_date >= '{{ var("min_date") }}'
  and sbl.event_date <= '{{ var("max_date") }}'
