-- The money-shot asset: margin-by-lane-and-customer, the number the new CFO
-- wants and the number Priya can't currently trust. Recomputes automatically
-- when its inputs update, gated on carrier_rate_validated's blocking check
-- passing -- see defs/transformation/marts/defs.yaml.
--
-- Joined on (event_date, lane, carrier), not just (event_date, lane): a
-- carrier with no rate data for a day has no row in `cost`, so its shipments
-- drop out of this model entirely for that day rather than being priced with
-- a silently-missing cost component. Blocked, not wrong.
with revenue as (
    select
        event_date,
        lane,
        carrier,
        customer_id,
        customer_name,
        sum(line_amount) as revenue
    from {{ ref('invoice_line_items') }}
    where event_date >= '{{ var("min_date") }}'
      and event_date <= '{{ var("max_date") }}'
    group by 1, 2, 3, 4, 5
),

cost as (
    select event_date, lane, carrier, allocated_cost
    from {{ ref('carrier_cost_allocation') }}
    where event_date >= '{{ var("min_date") }}'
      and event_date <= '{{ var("max_date") }}'
)

select
    r.event_date,
    r.lane,
    r.carrier,
    r.customer_id,
    r.customer_name,
    r.revenue,
    c.allocated_cost,
    r.revenue - c.allocated_cost as margin
from revenue r
inner join cost c
    on r.event_date = c.event_date
    and r.lane = c.lane
    and r.carrier = c.carrier
