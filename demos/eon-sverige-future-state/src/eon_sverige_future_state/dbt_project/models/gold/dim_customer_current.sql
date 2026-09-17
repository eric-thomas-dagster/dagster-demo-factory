-- Current-state customer dimension. One row per customer_id, from the
-- bronze customer_records feed. Downstream of the Databricks
-- ingest_customer_records_to_bronze job.
select
    customer_id,
    meter_id,
    region_code,
    region_name,
    account_open_date,
    switching_status,
    retailer_code
from {{ source('bronze', 'customer_records') }}
where customer_id is not null
