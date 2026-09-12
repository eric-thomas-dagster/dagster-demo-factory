-- The single lineage graph the four independently-owned business units land
-- in -- the direct answer to Wauter's "spiderweb that is only getting more
-- tangled." One consolidated activity ledger across battery_materials,
-- catalysis, recycling, and specialty_materials, normalized to a common
-- shape so corporate can see all four in one place instead of stitching
-- them together by hand.
select
    'battery_materials' as business_unit,
    production_date as activity_date,
    round(sum(output_kg), 1) as output_kg,
    count(*) as record_count
from {{ ref('stg_battery_materials') }}
group by production_date

union all

select
    'catalysis' as business_unit,
    feed_date as activity_date,
    round(sum(throughput_units), 1) as output_kg,
    count(*) as record_count
from {{ ref('stg_catalysis_ac') }}
group by feed_date

union all

select
    'recycling' as business_unit,
    processed_date as activity_date,
    round(sum(recovered_material_kg), 1) as output_kg,
    count(*) as record_count
from {{ ref('stg_recycling') }}
group by processed_date

union all

select
    'specialty_materials' as business_unit,
    production_date as activity_date,
    round(sum(output_kg), 1) as output_kg,
    count(*) as record_count
from {{ ref('stg_specialty_materials') }}
group by production_date
