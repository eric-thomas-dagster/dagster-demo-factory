-- One row per funded loan application, with borrower and product context.
select
    a.application_id,
    a.application_date,
    a.product_line,
    p.product_line_name,
    a.business_id,
    a.business_state,
    a.requested_amount,
    a.funded_amount,
    a.apr,
    a.term_months,
    b.business_credit_score,
    b.personal_credit_score
from {{ ref('stg_loan_applications') }} a
left join {{ ref('dim_borrower') }} b on a.business_id = b.business_id
left join {{ ref('dim_product_line') }} p on a.product_line = p.product_line
where a.funding_status = 'funded'
