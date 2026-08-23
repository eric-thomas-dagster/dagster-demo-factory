{{
    config(
        unique_key=['margin_date', 'customer_id', 'lane_code', 'carrier'],
        meta={
            'dagster': {
                'group': 'reporting',
                'auto_materialize_policy': {'type': 'eager'},
            }
        },
    )
}}

-- The money-shot asset: what the CFO wants and what Priya can't currently
-- trust. Blocked, not silently wrong, for any date where carrier_rate_arrival
-- fails -- both invoice_line_items and carrier_cost_allocation are
-- downstream of that test and get skipped by dbt for the affected partition,
-- so this model has nothing bad to join against.
with line_costs as (
    select
        ili.invoice_date as margin_date,
        ili.customer_id,
        ili.lane_code,
        ili.carrier,
        ili.line_amount_usd,
        ili.weight_lbs,
        cca.avg_rate_amount_usd
    from {{ ref('invoice_line_items') }} ili
    left join {{ ref('carrier_cost_allocation') }} cca
        on ili.carrier = cca.carrier
        and ili.invoice_date = cca.cost_date
    where ili.invoice_date >= '{{ var("min_date") }}'
      and ili.invoice_date < '{{ var("max_date") }}'
)
select
    lc.margin_date,
    lc.customer_id,
    a.account_name,
    lc.lane_code,
    lc.carrier,
    sum(lc.line_amount_usd) as total_invoiced_usd,
    sum(lc.weight_lbs / 100.0 * lc.avg_rate_amount_usd) as estimated_carrier_cost_usd,
    sum(lc.line_amount_usd) - sum(lc.weight_lbs / 100.0 * lc.avg_rate_amount_usd) as estimated_margin_usd
from line_costs lc
left join {{ source('raw', 'salesforce_accounts') }} a
    on lc.customer_id = a.customer_id
group by 1, 2, 3, 4, 5
