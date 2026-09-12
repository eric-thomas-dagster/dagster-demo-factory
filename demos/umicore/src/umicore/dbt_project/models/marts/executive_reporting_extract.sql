-- Corporate executive rollup across all four business units -- the third
-- job in Anthony's sequential-chain example ("if the first runs long, the
-- other two sit stale until the next scheduled window"). Depends on
-- `fct_cross_bu_consolidated_ledger`, which depends on
-- `stg_specialty_materials`, which depends on the specialty_materials raw
-- feed -- the chain the money-shot eager-automation demo walks end to end.
select
    activity_date,
    count(distinct business_unit) as reporting_business_units,
    round(sum(output_kg), 1) as total_output_kg,
    sum(record_count) as total_records
from {{ ref('fct_cross_bu_consolidated_ledger') }}
group by activity_date
