-- One row per borrower: exposure across every contract, echoing the
-- "Customer Data Platform" framing from SFS's own job posting.
select
    b.borrower_id,
    b.borrower_state,
    b.bureau_score,
    b.bureau_name,
    count(distinct f.contract_id) as active_contract_count,
    sum(f.outstanding_balance) as total_exposure,
    bool_or(coalesce(d.delinquency_severity, '') = 'severe') as has_severe_delinquency
from {{ ref('dim_borrower') }} b
left join {{ ref('fact_loan_portfolio') }} f using (borrower_id)
left join {{ ref('stg_delinquency_events') }} d on d.contract_id = f.contract_id
group by b.borrower_id, b.borrower_state, b.bureau_score, b.bureau_name
