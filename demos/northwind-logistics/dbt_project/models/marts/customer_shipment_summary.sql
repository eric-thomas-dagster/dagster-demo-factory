{{
    config(
        unique_key=['summary_date', 'customer_id'],
    )
}}

-- Ties shipment + invoice activity back to the Salesforce account record --
-- the CRM context a customer-facing team would want alongside the numbers.
with shipment_agg as (
    select
        event_date as summary_date,
        customer_id,
        count(*) as shipment_count,
        sum(weight_lbs) as total_weight_lbs
    from {{ ref('shipment_events_clean') }}
    where event_date >= '{{ var("min_date") }}'
      and event_date < '{{ var("max_date") }}'
    group by 1, 2
),
invoice_agg as (
    select
        invoice_date as summary_date,
        customer_id,
        sum(line_amount_usd) as total_invoiced_usd
    from {{ ref('invoice_line_items') }}
    where invoice_date >= '{{ var("min_date") }}'
      and invoice_date < '{{ var("max_date") }}'
    group by 1, 2
)
select
    coalesce(s.summary_date, i.summary_date) as summary_date,
    coalesce(s.customer_id, i.customer_id) as customer_id,
    a.account_name,
    a.annual_shipment_volume_tier,
    s.shipment_count,
    s.total_weight_lbs,
    i.total_invoiced_usd
from shipment_agg s
full outer join invoice_agg i
    on s.summary_date = i.summary_date
    and s.customer_id = i.customer_id
left join {{ source('raw', 'salesforce_accounts') }} a
    on coalesce(s.customer_id, i.customer_id) = a.customer_id
