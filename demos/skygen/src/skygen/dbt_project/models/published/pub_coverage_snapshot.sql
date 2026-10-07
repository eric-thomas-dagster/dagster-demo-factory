select tenant_id, report_date, plan_type, count(*) as members,
       sum(case when is_active then 1 else 0 end) as active_members,
       max(source_extracted_at) as source_extracted_at
from {{ ref('tfm_member_coverage') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
group by tenant_id, report_date, plan_type
