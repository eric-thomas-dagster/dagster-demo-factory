{{
    config(
        unique_key=['ship_date', 'carrier'],
    )
}}

-- Per-carrier scorecard: volume against average transit time and rate.
-- A natural ask for Priya's team once lineage exists to trust it.
with lane_summary as (
    select
        ship_date,
        carrier,
        sum(shipment_count) as shipment_count,
        sum(total_weight_lbs) as total_weight_lbs
    from {{ ref('shipments_by_lane') }}
    where ship_date >= '{{ var("min_date") }}'
      and ship_date < '{{ var("max_date") }}'
    group by 1, 2
)
select
    ls.ship_date,
    ls.carrier,
    ls.shipment_count,
    ls.total_weight_lbs,
    cca.avg_transit_days,
    cca.avg_rate_amount_usd
from lane_summary ls
left join {{ ref('carrier_cost_allocation') }} cca
    on ls.carrier = cca.carrier
    and ls.ship_date = cca.cost_date
