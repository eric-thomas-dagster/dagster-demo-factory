select claim_id, tenant_id, report_date, member_id, service_type,
       billed_amount, paid_amount, source_extracted_at
from {{ source('client_landing', 'vision_claims') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
