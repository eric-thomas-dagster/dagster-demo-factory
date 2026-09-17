-- Daily meter fact: one row per (meter_id, read_date), aggregating any
-- multi-read days. In the fixture there is one read per meter per day, so
-- this is a pass-through count-1 aggregate over silver -- which lets the
-- silver <-> gold row-count reconciliation macro verify no rows drop.
select
    meter_id,
    customer_id,
    region_code,
    read_date,
    max(kwh) as kwh,
    avg(signal_rsrp_dbm) as avg_signal_rsrp_dbm,
    avg(signal_csq) as avg_signal_csq,
    count(*) as read_count
from {{ ref('stg_meter_reads') }}
group by meter_id, customer_id, region_code, read_date
