-- One row per shipment. Revenue is the customer's daily NetSuite invoice
-- amount split evenly across their shipments that day -- a simplification
-- of Northwind's real rating logic, plausible enough for the demo without
-- claiming to reproduce actual invoice math.
{{ config(unique_key='event_date') }}

with customer_daily_revenue as (
    select
        customer_id,
        event_date,
        sum(amount) as daily_invoice_amount
    from {{ ref('stg_netsuite_gl_entries') }}
    where event_date >= '{{ var("min_date") }}'
      and event_date <= '{{ var("max_date") }}'
    group by 1, 2
),

shipments as (
    select *
    from {{ ref('stg_shipment_events') }}
    where event_date >= '{{ var("min_date") }}'
      and event_date <= '{{ var("max_date") }}'
),

shipment_counts as (
    select customer_id, event_date, count(*) as shipment_count
    from shipments
    group by 1, 2
)

select
    s.shipment_id,
    s.event_date,
    s.customer_id,
    a.customer_name,
    a.segment,
    s.lane,
    s.carrier,
    coalesce(r.daily_invoice_amount, 0) / nullif(c.shipment_count, 0) as line_amount
from shipments s
left join shipment_counts c
    on s.customer_id = c.customer_id and s.event_date = c.event_date
left join customer_daily_revenue r
    on s.customer_id = r.customer_id and s.event_date = r.event_date
left join {{ ref('stg_salesforce_accounts') }} a
    on s.customer_id = a.customer_id
