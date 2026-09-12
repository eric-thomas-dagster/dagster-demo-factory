-- Daily battery-materials production inputs feeding the EU Battery
-- Regulation carbon-footprint declaration -- Dagster's role is upstream
-- data reliability feeding that process, never the footprint calculation
-- itself (out of scope per the brief). Full-refresh over the whole
-- accumulated raw window; Dagster's own partition bookkeeping on this
-- asset is independent of dbt's execution grain here (same pattern used by
-- demos/rvu-tempcover, demos/kapitus, demos/partners-fcu).
select
    production_date,
    count(*) as batch_count,
    round(sum(output_kg), 1) as total_output_kg
from {{ ref('stg_battery_materials') }}
group by production_date
