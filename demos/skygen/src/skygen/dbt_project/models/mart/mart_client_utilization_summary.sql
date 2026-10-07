select tenant_id, report_date, benefit_line,
       count(*) as claims, count(distinct member_id) as members_with_claims,
       sum(billed_amount) as billed_amount, sum(paid_amount) as paid_amount
from {{ ref('pub_member_claims') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
group by tenant_id, report_date, benefit_line
