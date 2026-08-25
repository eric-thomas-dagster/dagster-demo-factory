-- The money-shot terminal asset: which contracts are eligible for the 2026
-- ABS securitization pool. `abs_pool_eligibility_completeness` (a blocking
-- Dagster asset check) refuses to let a pool with incomplete loan-tape
-- fields be treated as ready for investor/rating-agency reporting.
select
    contract_id,
    product_type,
    dealer_id,
    borrower_id,
    outstanding_balance,
    apr,
    contract_date,
    (
        contract_id is not null
        and dealer_id is not null
        and outstanding_balance is not null
        and outstanding_balance > 0
    ) as pool_eligible
from {{ ref('fact_loan_portfolio') }}
