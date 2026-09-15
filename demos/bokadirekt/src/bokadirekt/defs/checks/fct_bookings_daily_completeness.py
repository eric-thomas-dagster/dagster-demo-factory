"""Blocking completeness check on the daily bookings fact, gating the
Python rollup (`specialist_utilization_daily`) downstream.

A bad day's data shouldn't silently roll forward into the utilization
rollup a specialist-ops person would actually look at -- the brief's own
gating requirement for this asset.
"""

import dagster as dg

from bokadirekt.demo_data.warehouse import demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["marts", "fct_bookings_daily"]),
    blocking=True,
    description=(
        "Fails when the materialized partition's date has zero booking_count "
        "-- blocks specialist_utilization_daily from computing on a day with "
        "no data."
    ),
)
def fct_bookings_daily_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    partition_key = context.partition_key if context.has_partition_key else None

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        if partition_key:
            row = conn.execute(
                "select booking_count from main_marts.fct_bookings_daily where booking_date = ?",
                [partition_key],
            ).fetchone()
        else:
            row = conn.execute(
                "select sum(booking_count) from main_marts.fct_bookings_daily"
            ).fetchone()
    finally:
        conn.close()

    booking_count = row[0] if row and row[0] is not None else 0
    passed = booking_count > 0
    return dg.AssetCheckResult(
        passed=passed,
        description=(
            f"partition {partition_key or '(all)'}: {booking_count} bookings on file."
        ),
        metadata={"partition_key": partition_key or "(all)", "booking_count": booking_count},
    )
