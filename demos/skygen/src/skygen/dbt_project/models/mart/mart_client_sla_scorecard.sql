-- One row per tenant per day: did the day's data land and publish before the
-- tenant's 06:00 UTC SLA deadline?
select tenant_id, report_date,
       count(*) as claims_published,
       sum(paid_amount) as paid_amount,
       max(source_extracted_at) as data_ready_at,
       cast(report_date as timestamp) + interval 1 day + interval 6 hour as sla_deadline,
       date_diff('minute', max(source_extracted_at),
                 cast(report_date as timestamp) + interval 1 day + interval 6 hour) as sla_slack_minutes,
       max(source_extracted_at) <= cast(report_date as timestamp) + interval 1 day + interval 6 hour as sla_met
from {{ ref('pub_member_claims') }}
where tenant_id = '{{ var('tenant') }}' and report_date = '{{ var('report_date') }}'
group by tenant_id, report_date
