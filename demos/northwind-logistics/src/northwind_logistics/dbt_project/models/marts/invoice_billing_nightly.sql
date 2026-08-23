{{
    config(
        unique_key=['billing_date'],
        meta={
            'dagster': {
                'group': 'reporting',
            }
        },
    )
}}

-- Must be materializable before the 6am ET finance close (see the schedule
-- in defs/schedules.py). gl_freight_revenue_usd is a whole-ledger running
-- total from NetSuite (GL entries aren't shipment-date-scoped), not a
-- per-day figure -- it's here for a reconciliation eyeball check, not an
-- exact per-partition match.
with invoice_totals as (
    select
        invoice_date as billing_date,
        count(distinct shipment_id) as shipment_count,
        sum(line_amount_usd) as total_invoiced_usd
    from {{ ref('invoice_line_items') }}
    where invoice_date >= '{{ var("min_date") }}'
      and invoice_date < '{{ var("max_date") }}'
    group by 1
),
gl_totals as (
    select sum(amount_usd) as gl_freight_revenue_usd
    from {{ source('raw', 'netsuite_gl_entries') }}
    where account_code = '4000-FREIGHT-REV'
)
select
    it.billing_date,
    it.shipment_count,
    it.total_invoiced_usd,
    gl.gl_freight_revenue_usd
from invoice_totals it
cross join gl_totals gl
