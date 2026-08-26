-- Exec-facing rollup: total funding activity across every product line, one row per day.
select
    application_date,
    sum(application_count) as total_applications,
    sum(funded_count) as total_funded,
    round(sum(funded_count)::double / nullif(sum(application_count), 0), 4) as overall_approval_rate,
    sum(total_funded_amount) as total_funded_amount
from {{ ref('portfolio_performance_by_product') }}
group by application_date
