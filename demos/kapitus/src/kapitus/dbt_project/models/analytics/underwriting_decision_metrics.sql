-- Funded vs. declined credit-score gap, one row per day -- the underwriting team's read on decision quality.
with scored as (
    select
        a.application_date,
        a.funding_status,
        c.business_credit_score
    from {{ ref('stg_loan_applications') }} a
    left join {{ ref('stg_credit_bureau_pulls') }} c
        on a.business_id = c.business_id
        and a.application_date = c.pull_date
        and a.product_line = c.product_line
)
select
    application_date,
    avg(business_credit_score) filter (where funding_status = 'funded') as funded_avg_business_score,
    avg(business_credit_score) filter (where funding_status = 'declined') as declined_avg_business_score,
    avg(business_credit_score) filter (where funding_status = 'funded')
        - avg(business_credit_score) filter (where funding_status = 'declined') as score_gap
from scored
group by application_date
