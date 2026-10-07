select c.claim_id, c.tenant_id, c.report_date, c.member_id, c.benefit_line, c.service_code,
       c.billed_amount, c.paid_amount, m.plan_type, m.is_active, c.source_extracted_at
from {{ ref('tfm_claims_unified') }} c
left join {{ ref('tfm_member_coverage') }} m
  on c.tenant_id = m.tenant_id and c.report_date = m.report_date and c.member_id = m.member_id
where c.tenant_id = '{{ var('tenant') }}' and c.report_date = '{{ var('report_date') }}'
