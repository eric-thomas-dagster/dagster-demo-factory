-- The nightly batch finance needs closed by 6am ET (see the schedule in
-- defs/transformation/marts/defs.yaml and the `invoice_batch_completeness`
-- check). Maps to: "we find out something broke when a customer emails
-- about a missing invoice."
{{ config(unique_key='event_date') }}

select
    event_date,
    count(distinct customer_id) as customers_billed,
    count(*) as line_item_count,
    sum(line_amount) as total_invoiced,
    current_timestamp as generated_at
from {{ ref('invoice_line_items') }}
where event_date >= '{{ var("min_date") }}'
  and event_date <= '{{ var("max_date") }}'
group by 1
