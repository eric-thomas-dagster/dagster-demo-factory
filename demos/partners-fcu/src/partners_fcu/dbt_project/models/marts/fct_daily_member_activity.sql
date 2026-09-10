-- Daily member transaction volume and dollar activity. Dagster's own
-- partition bookkeeping for this asset is independent of dbt's execution
-- grain here -- this is a small full-refresh transform over the whole
-- accumulated raw window, not an incremental build scoped to one
-- partition (same pattern used by demos/rvu-tempcover and demos/kapitus).
select
    transaction_date,
    count(*) as transaction_count,
    count(distinct member_id) as active_member_count,
    round(sum(amount), 2) as total_amount
from {{ ref('stg_member_transactions') }}
group by transaction_date
