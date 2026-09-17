-- Typed silver view over the bronze Delta meter-reads table. Filters to
-- valid meter IDs and bounded read date range. Column names match a
-- plausible bronze Delta schema so switching profiles from DuckDB to
-- Databricks is a target change, not a rewrite.
select
    read_id,
    meter_id,
    customer_id,
    region_code,
    cast(read_date as date) as read_date,
    kwh,
    signal_rsrp_dbm,
    signal_csq
from {{ source('bronze', 'meter_reads') }}
where meter_id is not null
  and customer_id is not null
  and read_date is not null
  and read_date >= '{{ var("window_start") }}'
  and read_date <= '{{ var("window_end") }}'
