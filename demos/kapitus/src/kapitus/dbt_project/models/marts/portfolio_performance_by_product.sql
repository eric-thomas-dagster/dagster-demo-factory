-- Daily application/approval performance, one row per (date, product_line).
select
    application_date,
    product_line,
    count(*) as application_count,
    sum(case when funding_status = 'funded' then 1 else 0 end) as funded_count,
    round(sum(case when funding_status = 'funded' then 1 else 0 end)::double / count(*), 4) as approval_rate,
    sum(coalesce(funded_amount, 0)) as total_funded_amount,
    avg(apr) filter (where funding_status = 'funded') as avg_apr,
    avg(funded_amount) filter (where funding_status = 'funded') as avg_funded_amount
from {{ ref('stg_loan_applications') }}
group by application_date, product_line
