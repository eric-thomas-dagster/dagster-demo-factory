-- Typed taxi zones -- the TLC LocationID lookup used to enrich every fact.
select
    zone_id,
    zone_name,
    borough,
    service_zone
from {{ source('raw', 'taxi_zone_lookup') }}
where zone_id is not null
