-- The declaration-facing extract, re-surfacing the same reliable,
-- check-gated battery-materials inputs under the name the compliance
-- process consumes. No footprint math lives here on purpose -- the demo's
-- job is proving the data feeding the EU Battery Regulation carbon-footprint
-- declaration is trustworthy, not computing the declaration itself
-- (explicitly out of scope per the brief).
select
    production_date,
    batch_count,
    total_output_kg
from {{ ref('fct_battery_carbon_footprint_inputs') }}
