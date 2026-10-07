select claim_id, tenant_id, report_date, member_id, 'dental' as benefit_line,
       procedure_code as service_code, billed_amount, paid_amount, source_extracted_at
from {{ ref('ing_dental_claims') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
union all
select claim_id, tenant_id, report_date, member_id, 'vision' as benefit_line,
       service_type as service_code, billed_amount, paid_amount, source_extracted_at
from {{ ref('ing_vision_claims') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
