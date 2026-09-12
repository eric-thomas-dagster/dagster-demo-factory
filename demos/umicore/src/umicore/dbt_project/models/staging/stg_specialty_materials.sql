-- Typed specialty-materials output.
select
    output_id,
    product_line,
    quality_grade,
    output_kg,
    cast(produced_on as date) as production_date
from {{ source('raw', 'specialty_materials_output') }}
