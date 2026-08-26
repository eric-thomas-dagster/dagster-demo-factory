-- Daily underwriting risk profile, one row per (date, product_line).
with joined as (
    select
        c.pull_date,
        c.product_line,
        c.business_id,
        c.business_credit_score,
        c.personal_credit_score,
        s.nsf_count_90d,
        s.cash_flow_score
    from {{ ref('stg_credit_bureau_pulls') }} c
    left join {{ ref('stg_bank_statement_data') }} s
        on c.business_id = s.business_id
        and c.pull_date = s.statement_date
        and c.product_line = s.product_line
)
select
    pull_date,
    product_line,
    count(*) as applicant_count,
    avg(business_credit_score) as avg_business_credit_score,
    avg(personal_credit_score) as avg_personal_credit_score,
    avg(cash_flow_score) as avg_cash_flow_score,
    sum(case when business_credit_score < 600 or coalesce(nsf_count_90d, 0) >= 2 then 1 else 0 end) as flagged_for_review_count
from joined
group by pull_date, product_line
