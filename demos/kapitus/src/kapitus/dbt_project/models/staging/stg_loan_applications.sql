-- Typed loan applications and their funding decisions.
select
    application_id,
    cast(application_date as date) as application_date,
    product_line,
    business_id,
    business_state,
    requested_amount,
    funding_status,
    funded_amount,
    apr,
    term_months
from {{ source('raw', 'loan_applications') }}
where application_date >= '{{ var("min_date") }}'
  and application_date <= '{{ var("max_date") }}'
