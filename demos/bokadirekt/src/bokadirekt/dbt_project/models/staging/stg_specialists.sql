-- Typed specialist roster. Columns are deliberately limited to what the
-- booking platform's specialist directory would expose -- no PII column
-- (name/contact details) beyond a display name, so the schema/PII shape
-- check downstream stays green while still asserting something real.
select
    specialist_id,
    specialist_name,
    region,
    service_category,
    cast(onboarded_date as date) as onboarded_date
from {{ source('raw', 'raw_specialists') }}
