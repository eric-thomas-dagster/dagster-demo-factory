-- Typed commercial credit bureau pulls.
select
    pull_id,
    cast(pull_date as date) as pull_date,
    product_line,
    business_id,
    bureau_name,
    business_credit_score,
    personal_credit_score,
    years_in_business,
    existing_debt_obligations
from {{ source('raw', 'credit_bureau_pulls') }}
where pull_date >= '{{ var("min_date") }}'
  and pull_date <= '{{ var("max_date") }}'
