"""Silver -- hand-written asset functions that type + filter the bronze rows.

Every `@asset` here is: partition context in, DuckDB `INSERT`/`CREATE` out.
Column names + projections match the parent project's dbt `stg_*` models so
row counts reconcile identically across the two demos.

Note the friction that dbt+DCC removed and this file re-introduces:
- Schema creation -- you write `CREATE SCHEMA IF NOT EXISTS main_silver`
- Partition window in SQL -- you interpolate `context.partition_key` +
  `context.partition_time_window.end` into a `WHERE ... BETWEEN` clause
- Idempotent partition writes -- you `DELETE WHERE partition-window` before
  `INSERT`, so re-runs don't double-count
- Table shape -- you `CREATE TABLE IF NOT EXISTS` with the full schema up
  front, because the first partition needs a target table to insert into
- Metadata -- you compute `row_count` yourself with a follow-up query
"""

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    asset,
)
from dagster_duckdb import DuckDBResource

from neighborhood_intelligence_taxi_python.partitions import MONTHLY_PARTITIONS


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    key=AssetKey(["silver", "stg_yellow_trips"]),
    group_name="silver",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey("source_yellow_trips")],
    description="Typed + null-filtered yellow taxi trips.",
)
def stg_yellow_trips(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    partition_start = context.partition_key
    partition_end = context.partition_time_window.end.strftime("%Y-%m-%d")

    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS main_silver.stg_yellow_trips (
                trip_id VARCHAR,
                vendor_id INTEGER,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE,
                pickup_location_id INTEGER,
                dropoff_location_id INTEGER,
                payment_type INTEGER,
                fare_amount DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                total_amount DOUBLE,
                pickup_date DATE,
                pickup_hour INTEGER
            )
            """
        )
        # Idempotent partition write: drop this month's rows first so re-runs
        # don't double-count. dbt's incremental materialization hides this.
        conn.execute(
            f"""
            DELETE FROM main_silver.stg_yellow_trips
            WHERE pickup_date >= DATE '{partition_start}'
              AND pickup_date <  DATE '{partition_end}'
            """
        )
        conn.execute(
            f"""
            INSERT INTO main_silver.stg_yellow_trips
            SELECT
                trip_id,
                vendor_id,
                CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
                CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_location_id,
                dropoff_location_id,
                payment_type,
                fare_amount,
                tip_amount,
                tolls_amount,
                total_amount,
                CAST(pickup_datetime AS DATE) AS pickup_date,
                EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour
            FROM raw.yellow_trips
            WHERE trip_id IS NOT NULL
              AND pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND pickup_location_id IS NOT NULL
              AND dropoff_location_id IS NOT NULL
              AND CAST(pickup_datetime AS DATE) >= DATE '{partition_start}'
              AND CAST(pickup_datetime AS DATE) <  DATE '{partition_end}'
            """
        )
        row_count = conn.execute(
            f"""
            SELECT COUNT(*) FROM main_silver.stg_yellow_trips
            WHERE pickup_date >= DATE '{partition_start}'
              AND pickup_date <  DATE '{partition_end}'
            """
        ).fetchone()[0]

    return MaterializeResult(
        metadata={
            "dagster/row_count": int(row_count),
            "partition_start": partition_start,
            "partition_end": partition_end,
        },
    )


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    key=AssetKey(["silver", "stg_taxi_zones"]),
    group_name="silver",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey("source_taxi_zone_lookup")],
    description="Typed taxi zone reference table -- one row per LocationID.",
)
def stg_taxi_zones(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    # Zone lookup is a small reference table: fully rebuilt each partition
    # (the shape dbt would call `materialized='table'`). No partition filter
    # to interpolate here -- it's the same 20 rows every run.
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
        conn.execute(
            """
            CREATE OR REPLACE TABLE main_silver.stg_taxi_zones AS
            SELECT
                zone_id,
                zone_name,
                borough,
                service_zone
            FROM raw.taxi_zone_lookup
            WHERE zone_id IS NOT NULL
            """
        )
        row_count = conn.execute(
            "SELECT COUNT(*) FROM main_silver.stg_taxi_zones"
        ).fetchone()[0]

    return MaterializeResult(
        metadata={"dagster/row_count": int(row_count)},
    )
