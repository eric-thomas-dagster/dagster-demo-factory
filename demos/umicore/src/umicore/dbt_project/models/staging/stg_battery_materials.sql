-- Typed battery-materials cathode production batches.
select
    cathode_batch_id,
    production_line,
    material_grade,
    output_kg,
    cast(produced_on as date) as production_date
from {{ source('raw', 'cathode_production_batch') }}
