select tenant_id, report_date, member_id, plan_type,
       coverage_status = 'active' as is_active, source_extracted_at
from {{ ref('ing_member_eligibility') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
