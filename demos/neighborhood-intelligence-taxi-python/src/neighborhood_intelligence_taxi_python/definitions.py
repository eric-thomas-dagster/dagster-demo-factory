"""Top-level wiring for the pure-Python counterfactual demo.

Every asset, check, schedule, and resource is enumerated here by hand -- no
component-driven autoload, no defs/ tree, no dbt. This is the manifest raw
Dagster gives you.

DuckDB is a single-writer file store; the default multiprocess executor
races workers to open `demo.duckdb` and blows up on the file lock. We force
in-process execution at the code-location level so `dg dev` materializations
stay serialized on one connection.
"""

from dagster import Definitions, definitions, in_process_executor
from dagster_duckdb import DuckDBResource

from neighborhood_intelligence_taxi_python.assets.bronze import (
    source_taxi_zone_lookup,
    source_yellow_trips,
)
from neighborhood_intelligence_taxi_python.assets.gold import (
    agg_daily_zone,
    agg_hourly_demand,
    agg_vendor_performance,
    fct_trips,
)
from neighborhood_intelligence_taxi_python.assets.report import (
    report_daily_summary,
    report_top_zones,
)
from neighborhood_intelligence_taxi_python.assets.silver import (
    stg_taxi_zones,
    stg_yellow_trips,
)
from neighborhood_intelligence_taxi_python.checks import (
    fct_trips_reconciliation,
    fct_trips_trip_id_not_null,
    fct_trips_trip_id_unique,
)
from neighborhood_intelligence_taxi_python.demo_data.bootstrap import (
    ensure_raw_fixtures_loaded,
)
from neighborhood_intelligence_taxi_python.schedules import (
    monthly_full_pipeline_schedule,
)
from neighborhood_intelligence_taxi_python.warehouse import demo_duckdb_path

ensure_raw_fixtures_loaded()


@definitions
def defs():
    loaded = Definitions(
        assets=[
            source_yellow_trips,
            source_taxi_zone_lookup,
            stg_yellow_trips,
            stg_taxi_zones,
            fct_trips,
            agg_daily_zone,
            agg_hourly_demand,
            agg_vendor_performance,
            report_daily_summary,
            report_top_zones,
        ],
        asset_checks=[
            fct_trips_trip_id_not_null,
            fct_trips_trip_id_unique,
            fct_trips_reconciliation,
        ],
        schedules=[monthly_full_pipeline_schedule],
        resources={
            "duckdb": DuckDBResource(database=demo_duckdb_path()),
        },
    )
    return Definitions.merge(loaded, Definitions(executor=in_process_executor))
