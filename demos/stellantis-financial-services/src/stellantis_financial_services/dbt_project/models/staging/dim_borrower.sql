-- One row per borrower, joined to their credit bureau pull. Each loan/lease
-- in this demo creates a new borrower (no re-financing modeled), so a
-- single left join is enough -- no dedup/latest-pull logic required.
with borrowers as (
    select borrower_id, borrower_state, contract_date as onboarded_date from {{ ref('stg_loan_originations') }}
    union
    select borrower_id, borrower_state, contract_date as onboarded_date from {{ ref('stg_lease_originations') }}
),
bureau as (
    select
        borrower_id,
        bureau_name,
        bureau_score,
        inquiry_count_6mo
    from {{ source('raw', 'credit_bureau_pull') }}
)
select
    b.borrower_id,
    b.borrower_state,
    b.onboarded_date,
    c.bureau_name,
    c.bureau_score,
    c.inquiry_count_6mo
from borrowers b
left join bureau c using (borrower_id)
