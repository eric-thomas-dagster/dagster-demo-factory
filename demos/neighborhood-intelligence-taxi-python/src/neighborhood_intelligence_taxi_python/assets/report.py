"""Report -- fan-out from gold. Two hand-written asset functions.

Both are `CREATE OR REPLACE TABLE` -- reporting tables rebuild whole each
partition run so downstream dashboards see a consistent snapshot.
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
    key=AssetKey(["report", "report_daily_summary"]),
    group_name="report",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey(["gold", "agg_daily_zone"])],
    description="Daily citywide summary -- one row per pickup_date.",
)
def report_daily_summary(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_report")
        conn.execute(
            """
            CREATE OR REPLACE TABLE main_report.report_daily_summary AS
            SELECT
                pickup_date,
                COUNT(DISTINCT pickup_location_id) AS active_zone_count,
                SUM(trip_count) AS trip_count,
                SUM(passenger_count) AS passenger_count,
                ROUND(SUM(total_distance_miles), 2) AS total_distance_miles,
                ROUND(SUM(total_fare_amount), 2) AS total_fare_amount,
                ROUND(SUM(total_tip_amount), 2) AS total_tip_amount,
                ROUND(SUM(total_revenue), 2) AS total_revenue,
                ROUND(SUM(total_revenue) / NULLIF(SUM(trip_count), 0), 2) AS revenue_per_trip
            FROM main_gold.agg_daily_zone
            GROUP BY pickup_date
            ORDER BY pickup_date
            """
        )
        row_count = conn.execute(
            "SELECT COUNT(*) FROM main_report.report_daily_summary"
        ).fetchone()[0]

    return MaterializeResult(metadata={"dagster/row_count": int(row_count)})


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    key=AssetKey(["report", "report_top_zones"]),
    group_name="report",
    kinds={"python", "duckdb"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey(["gold", "fct_trips"])],
    description="Ranked pickup zones by trip volume across the loaded window.",
)
def report_top_zones(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_report")
        conn.execute(
            """
            CREATE OR REPLACE TABLE main_report.report_top_zones AS
            SELECT
                pickup_location_id,
                pickup_zone_name,
                pickup_borough,
                COUNT(*) AS trip_count,
                ROUND(SUM(fare_amount), 2) AS total_fare_amount,
                ROUND(SUM(tip_amount), 2) AS total_tip_amount,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(AVG(trip_distance), 2) AS avg_trip_distance_miles,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS trip_count_rank
            FROM main_gold.fct_trips
            WHERE pickup_zone_name IS NOT NULL
            GROUP BY pickup_location_id, pickup_zone_name, pickup_borough
            """
        )
        row_count = conn.execute(
            "SELECT COUNT(*) FROM main_report.report_top_zones"
        ).fetchone()[0]

    return MaterializeResult(metadata={"dagster/row_count": int(row_count)})
