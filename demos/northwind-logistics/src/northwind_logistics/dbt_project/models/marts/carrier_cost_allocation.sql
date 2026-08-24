-- Recomputes automatically when either upstream updates (see the
-- check-gated AutomationCondition in defs/transformation/marts/defs.yaml) --
-- this is the asset that has to catch up on its own once the healed
-- carrier partition is rematerialized, with nobody clicking it. An inner
-- join means a (day, carrier, lane) with no arrived rate data simply has
-- no row here -- blocked, not wrong.
select
    sbl.event_date,
    sbl.lane,
    sbl.carrier,
    sbl.total_miles,
    cr.rate_per_mile,
    cr.fuel_surcharge_pct,
    sbl.total_miles * cr.rate_per_mile * (1 + cr.fuel_surcharge_pct) as allocated_cost
from {{ ref('shipments_by_lane') }} sbl
inner join {{ ref('carrier_rate_validated') }} cr
    on sbl.event_date = cr.event_date
    and sbl.lane = cr.lane
    and sbl.carrier = cr.carrier
where sbl.event_date >= '{{ var("min_date") }}'
  and sbl.event_date <= '{{ var("max_date") }}'
