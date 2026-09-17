-- Typed silver view over the bronze Delta grid-load-telemetry table.
select
    telemetry_id,
    region_code,
    cast(telemetry_date as date) as telemetry_date,
    hour_of_day,
    load_mw,
    voltage_v
from {{ source('bronze', 'grid_load_telemetry') }}
where region_code is not null
  and telemetry_date is not null
  and hour_of_day between 0 and 23
  and telemetry_date >= '{{ var("window_start") }}'
  and telemetry_date <= '{{ var("window_end") }}'
