-- Hourly grid fact per region -- the training input for the
-- grid_load_forecast_model (Azure ML MLflow).
select
    region_code,
    telemetry_date,
    hour_of_day,
    avg(load_mw) as avg_load_mw,
    max(load_mw) as peak_load_mw,
    min(voltage_v) as min_voltage_v,
    max(voltage_v) as max_voltage_v,
    count(*) as sample_count
from {{ ref('stg_grid_telemetry') }}
group by region_code, telemetry_date, hour_of_day
