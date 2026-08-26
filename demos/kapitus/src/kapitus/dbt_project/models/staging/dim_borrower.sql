-- One row per business, with its most recent underwriting signals.
with applications as (
    select
        business_id,
        min(application_date) as first_application_date,
        max(application_date) as last_application_date,
        count(*) as application_count,
        any_value(business_state) as business_state
    from {{ ref('stg_loan_applications') }}
    group by business_id
),
latest_bureau as (
    select business_id, business_credit_score, personal_credit_score, years_in_business
    from (
        select
            business_id, business_credit_score, personal_credit_score, years_in_business,
            row_number() over (partition by business_id order by pull_date desc) as rn
        from {{ ref('stg_credit_bureau_pulls') }}
    )
    where rn = 1
),
latest_statement as (
    select business_id, cash_flow_score
    from (
        select
            business_id, cash_flow_score,
            row_number() over (partition by business_id order by statement_date desc) as rn
        from {{ ref('stg_bank_statement_data') }}
    )
    where rn = 1
)
select
    a.business_id,
    a.business_state,
    a.first_application_date,
    a.last_application_date,
    a.application_count,
    b.business_credit_score,
    b.personal_credit_score,
    b.years_in_business,
    s.cash_flow_score
from applications a
left join latest_bureau b on a.business_id = b.business_id
left join latest_statement s on a.business_id = s.business_id
