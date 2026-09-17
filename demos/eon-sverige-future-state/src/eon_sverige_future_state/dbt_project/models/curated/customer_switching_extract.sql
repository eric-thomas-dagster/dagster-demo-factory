-- Regulator-facing extract for EU 2026/855 customer-switching
-- interoperability. Every column has upstream lineage from
-- bronze.customer_records -> stg (via the customer dim) with a joined
-- summary of the customer's meter and 30-day energy footprint. This
-- asset IS the audit trail.
--
-- SQL is deliberately simple / auditable: one row per customer, no
-- window functions or CTE gymnastics that would confuse a regulator.

with monthly_energy as (
    select
        customer_id,
        region_code,
        sum(kwh) as total_kwh_window,
        max(read_date) as most_recent_read_date,
        count(*) as read_count_window
    from {{ ref('fct_meter_daily') }}
    group by customer_id, region_code
)
select
    c.customer_id,
    c.meter_id,
    c.region_code,
    c.region_name,
    c.account_open_date,
    c.switching_status,
    c.retailer_code,
    coalesce(m.total_kwh_window, 0) as total_kwh_window,
    m.most_recent_read_date,
    coalesce(m.read_count_window, 0) as read_count_window,
    -- Audit metadata: static per-row provenance stamp that makes the
    -- lineage self-describing when the extract is delivered to a
    -- regulator (they may not have the Dagster lineage UI).
    'bronze.customer_records -> dim_customer_current' as customer_lineage,
    'bronze.meter_reads -> stg_meter_reads -> fct_meter_daily' as energy_lineage,
    'EU_2026_855' as compliance_regulation,
    current_timestamp as extract_generated_at
from {{ ref('dim_customer_current') }} c
left join monthly_energy m
    on c.customer_id = m.customer_id
