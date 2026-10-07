select tenant_id, report_date, member_id, plan_type, coverage_status, source_extracted_at
from {{ source('client_landing', 'member_eligibility') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
