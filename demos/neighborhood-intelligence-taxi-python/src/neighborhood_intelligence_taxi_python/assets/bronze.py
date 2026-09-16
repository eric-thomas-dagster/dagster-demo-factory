"""Bronze external assets -- observation-only declarations of upstream tables.

`AssetSpec` is Dagster's way of declaring "this asset exists somewhere Dagster
didn't create, and we want it on the lineage graph." No function, no execution,
no ingestion -- just a declaration.

`kinds={"bigquery"}` badges these as BigQuery in the UI because the specs
represent the real `bigquery-public-data.new_york_taxi_trips.*` public tables
the pipeline would consume in a non-demo deployment. Demo mode reads the same
row shapes out of the local DuckDB `raw` schema seeded by demo_data/bootstrap.py.
"""

from dagster import AssetKey, AssetSpec

source_yellow_trips = AssetSpec(
    key=AssetKey("source_yellow_trips"),
    group_name="bronze",
    kinds={"bigquery"},
    owners=["team:nit-data-platform"],
    description="NYC TLC Yellow Taxi trips (2022) -- public dataset.",
    metadata={
        "bigquery/table": "bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022",
        "owner_team": "team:nit-data-platform",
        "tier": "tier_1",
        "domain": "trips",
    },
)

source_taxi_zone_lookup = AssetSpec(
    key=AssetKey("source_taxi_zone_lookup"),
    group_name="bronze",
    kinds={"bigquery"},
    owners=["team:nit-data-platform"],
    description="TLC Taxi Zone lookup -- one row per LocationID.",
    metadata={
        "bigquery/table": "bigquery-public-data.new_york_taxi_trips.taxi_zone_geom",
        "owner_team": "team:nit-data-platform",
        "tier": "tier_1",
        "domain": "reference",
    },
)
