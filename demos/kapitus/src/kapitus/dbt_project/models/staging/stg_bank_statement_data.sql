-- Typed OCR-derived bank statement analysis.
select
    statement_id,
    cast(statement_date as date) as statement_date,
    product_line,
    business_id,
    avg_daily_balance,
    nsf_count_90d,
    monthly_revenue_estimate,
    cash_flow_score
from {{ source('raw', 'bank_statement_data') }}
where statement_date >= '{{ var("min_date") }}'
  and statement_date <= '{{ var("max_date") }}'
