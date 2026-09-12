-- Typed recycling batch yield.
select
    batch_id,
    recycling_line,
    input_material,
    recovered_material_kg,
    yield_pct,
    cast(processed_on as date) as processed_date
from {{ source('raw', 'recycling_batch_yield') }}
