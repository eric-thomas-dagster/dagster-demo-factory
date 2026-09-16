"""Gold -- fact table + three aggregates, all hand-written asset functions.

`fct_trips` fans in from silver: joins typed trips to typed zones twice
(pickup + dropoff). The three aggregates each roll up `fct_trips`.

Same partition-scoped upsert pattern as silver: DELETE the partition window,
then INSERT the recomputed rows. This is the shape a dbt incremental model
handles with `unique_key` + `is_incremental()` -- here you write it yourself.

Aggregates are rebuilt whole rather than partition-scoped because their
grain (e.g. `agg_vendor_performance` groups by vendor_id across the entire
window) makes partition-scoped upserts less natural. dbt would let you
declare a `materialized: table` config; here you write `CREATE OR REPLACE`.
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
    key=AssetKey(["gold", "fct_trips"]),
    group_name="gold",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[
        AssetKey(["silver", "stg_yellow_trips"]),
        AssetKey(["silver", "stg_taxi_zones"]),
    ],
    description=(
        "Enriched trip fact: one row per completed yellow-taxi trip, joined "
        "to pickup + dropoff zone metadata. `trip_id` is the natural key."
    ),
)
def fct_trips(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    partition_start = context.partition_key
    partition_end = context.partition_time_window.end.strftime("%Y-%m-%d")

    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_gold")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS main_gold.fct_trips (
                trip_id VARCHAR,
                vendor_id INTEGER,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                pickup_date DATE,
                pickup_hour INTEGER,
                passenger_count INTEGER,
                trip_distance DOUBLE,
                pickup_location_id INTEGER,
                pickup_zone_name VARCHAR,
                pickup_borough VARCHAR,
                pickup_service_zone VARCHAR,
                dropoff_location_id INTEGER,
                dropoff_zone_name VARCHAR,
                dropoff_borough VARCHAR,
                dropoff_service_zone VARCHAR,
                payment_type INTEGER,
                fare_amount DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                total_amount DOUBLE,
                trip_duration_minutes DOUBLE
            )
            """
        )
        conn.execute(
            f"""
            DELETE FROM main_gold.fct_trips
            WHERE pickup_date >= DATE '{partition_start}'
              AND pickup_date <  DATE '{partition_end}'
            """
        )
        conn.execute(
            f"""
            INSERT INTO main_gold.fct_trips
            SELECT
                t.trip_id,
                t.vendor_id,
                t.pickup_datetime,
                t.dropoff_datetime,
                t.pickup_date,
                t.pickup_hour,
                t.passenger_count,
                t.trip_distance,
                t.pickup_location_id,
                pz.zone_name AS pickup_zone_name,
                pz.borough AS pickup_borough,
                pz.service_zone AS pickup_service_zone,
                t.dropoff_location_id,
                dz.zone_name AS dropoff_zone_name,
                dz.borough AS dropoff_borough,
                dz.service_zone AS dropoff_service_zone,
                t.payment_type,
                t.fare_amount,
                t.tip_amount,
                t.tolls_amount,
                t.total_amount,
                EXTRACT(EPOCH FROM (t.dropoff_datetime - t.pickup_datetime)) / 60.0
                    AS trip_duration_minutes
            FROM main_silver.stg_yellow_trips t
            LEFT JOIN main_silver.stg_taxi_zones pz
                ON t.pickup_location_id = pz.zone_id
            LEFT JOIN main_silver.stg_taxi_zones dz
                ON t.dropoff_location_id = dz.zone_id
            WHERE t.pickup_date >= DATE '{partition_start}'
              AND t.pickup_date <  DATE '{partition_end}'
            """
        )
        row_count = conn.execute(
            f"""
            SELECT COUNT(*) FROM main_gold.fct_trips
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
    key=AssetKey(["gold", "agg_daily_zone"]),
    group_name="gold",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey(["gold", "fct_trips"])],
    description="Daily pickup metrics per (pickup_date, pickup_zone).",
)
def agg_daily_zone(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    # Rebuilt whole each partition: aggregate rows span the entire loaded
    # window, so a partition-scoped upsert would leave stale months behind.
    # dbt's `materialized: table` config makes this a one-line choice.
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_gold")
        conn.execute(
            """
            CREATE OR REPLACE TABLE main_gold.agg_daily_zone AS
            SELECT
                pickup_date,
                pickup_location_id,
                pickup_zone_name,
                pickup_borough,
                COUNT(*) AS trip_count,
                SUM(passenger_count) AS passenger_count,
                ROUND(SUM(trip_distance), 2) AS total_distance_miles,
                ROUND(SUM(fare_amount), 2) AS total_fare_amount,
                ROUND(SUM(tip_amount), 2) AS total_tip_amount,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
            FROM main_gold.fct_trips
            GROUP BY pickup_date, pickup_location_id, pickup_zone_name, pickup_borough
            """
        )
        row_count = conn.execute(
            "SELECT COUNT(*) FROM main_gold.agg_daily_zone"
        ).fetchone()[0]

    return MaterializeResult(metadata={"dagster/row_count": int(row_count)})


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    key=AssetKey(["gold", "agg_hourly_demand"]),
    group_name="gold",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey(["gold", "fct_trips"])],
    description="Hour-of-day demand curve per pickup borough.",
)
def agg_hourly_demand(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_gold")
        conn.execute(
            """
            CREATE OR REPLACE TABLE main_gold.agg_hourly_demand AS
            SELECT
                pickup_borough,
                pickup_hour,
                COUNT(*) AS trip_count,
                ROUND(AVG(trip_distance), 2) AS avg_trip_distance_miles,
                ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
                ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
            FROM main_gold.fct_trips
            WHERE pickup_borough IS NOT NULL
            GROUP BY pickup_borough, pickup_hour
            """
        )
        row_count = conn.execute(
            "SELECT COUNT(*) FROM main_gold.agg_hourly_demand"
        ).fetchone()[0]

    return MaterializeResult(metadata={"dagster/row_count": int(row_count)})


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    key=AssetKey(["gold", "agg_vendor_performance"]),
    group_name="gold",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey(["gold", "fct_trips"])],
    description="Per-vendor economics: CMT (1) vs VeriFone (2).",
)
def agg_vendor_performance(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_gold")
        conn.execute(
            """
            CREATE OR REPLACE TABLE main_gold.agg_vendor_performance AS
            SELECT
                vendor_id,
                COUNT(*) AS trip_count,
                SUM(passenger_count) AS passenger_count,
                ROUND(SUM(fare_amount), 2) AS total_fare_amount,
                ROUND(SUM(tip_amount), 2) AS total_tip_amount,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(AVG(trip_distance), 2) AS avg_trip_distance_miles,
                ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes,
                ROUND(SUM(tip_amount) / NULLIF(SUM(fare_amount), 0), 4) AS tip_to_fare_ratio
            FROM main_gold.fct_trips
            GROUP BY vendor_id
            """
        )
        row_count = conn.execute(
            "SELECT COUNT(*) FROM main_gold.agg_vendor_performance"
        ).fetchone()[0]

    return MaterializeResult(metadata={"dagster/row_count": int(row_count)})
