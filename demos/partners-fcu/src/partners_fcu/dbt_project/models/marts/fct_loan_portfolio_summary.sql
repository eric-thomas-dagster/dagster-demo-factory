-- Daily loan origination volume and principal, by loan type.
select
    origination_date,
    loan_type,
    count(*) as loan_count,
    round(sum(principal_amount), 2) as total_principal_amount
from {{ ref('stg_loan_originations') }}
group by origination_date, loan_type
