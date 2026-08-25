-- One row per active loan or lease contract, with dealer + borrower context.
with loans as (
    select
        loan_id as contract_id,
        'auto_loan' as product_type,
        contract_date,
        dealer_id,
        borrower_id,
        amount_financed as outstanding_balance,
        apr,
        term_months
    from {{ ref('stg_loan_originations') }}
),
leases as (
    select
        lease_id as contract_id,
        'lease' as product_type,
        contract_date,
        dealer_id,
        borrower_id,
        capitalized_cost as outstanding_balance,
        cast(null as double) as apr,
        term_months
    from {{ ref('stg_lease_originations') }}
),
contracts as (
    select * from loans union all select * from leases
)
select
    c.contract_id,
    c.product_type,
    c.contract_date,
    c.dealer_id,
    d.dealer_name,
    d.dealer_region,
    c.borrower_id,
    b.bureau_score,
    c.outstanding_balance,
    c.apr,
    c.term_months
from contracts c
left join {{ ref('dim_dealer') }} d using (dealer_id)
left join {{ ref('dim_borrower') }} b using (borrower_id)
