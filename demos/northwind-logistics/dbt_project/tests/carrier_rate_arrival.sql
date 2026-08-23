{{ config(meta={'dagster': {'ref': {'name': 'carrier_rate_validated'}}}) }}

-- BLOCKING severity: this is what "Regional LTL carrier B rate data has not
-- arrived" looks like as a real dbt test rather than a green Airflow box.
-- `dbt build` skips every node downstream of a failed test in the same
-- invocation, so invoice_line_items, carrier_cost_allocation, and
-- margin_by_lane_customer never compute a wrong number for this partition --
-- they simply don't compute at all until the partition is healed.
select
    '{{ var("min_date") }}' as rate_date,
    'regional_ltl_b' as carrier,
    'Regional LTL carrier B rate data has not arrived for this partition; downstream margin is blocked, not silently wrong.' as failure_reason
where not exists (
    select 1
    from {{ ref('carrier_rate_validated') }}
    where carrier = 'regional_ltl_b'
      and rate_date >= '{{ var("min_date") }}'
      and rate_date < '{{ var("max_date") }}'
)
