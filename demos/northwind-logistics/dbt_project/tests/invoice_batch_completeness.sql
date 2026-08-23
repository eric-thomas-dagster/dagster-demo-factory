{{ config(meta={'dagster': {'ref': {'name': 'invoice_billing_nightly'}}}) }}

-- Maps to: "we find out something broke when a customer emails about a
-- missing invoice." An empty nightly batch fails loud here, before finance
-- ever opens Looker.
select
    billing_date,
    shipment_count,
    'Invoice batch for this date has zero shipments -- finance cannot close nightly billing.' as failure_reason
from {{ ref('invoice_billing_nightly') }}
where billing_date >= '{{ var("min_date") }}'
  and billing_date < '{{ var("max_date") }}'
  and (shipment_count is null or shipment_count = 0)
