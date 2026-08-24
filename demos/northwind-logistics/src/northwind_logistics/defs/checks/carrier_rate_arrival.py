"""The blocking check: freight rate data lands late ~15% of the time.

Blocking so `margin_by_lane_customer` and `carrier_cost_allocation` refuse to
compute a wrong number for a carrier whose rate data never arrived, rather
than reproducing Northwind's current situation (a wrong number, published
with confidence) with nicer styling.
"""

import dagster as dg

from northwind_logistics.demo_data.state import EXPECTED_CARRIERS
from northwind_logistics.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["staging", "stg_carrier_rates"]),
    blocking=True,
    description="Fails when a carrier's rate feed hasn't arrived for the partition's day.",
)
def carrier_rate_arrival(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        present = {
            row[0]
            for row in conn.execute(
                "select distinct carrier from main_staging.stg_carrier_rates where event_date = ?",
                [event_date],
            ).fetchall()
        }
    finally:
        conn.close()

    missing = sorted(set(EXPECTED_CARRIERS) - present)
    if missing:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Rate data has not arrived for {', '.join(missing)} on {event_date}. "
                "Margin for this partition is blocked for those carriers, not silently wrong."
            ),
            metadata={"missing_carriers": dg.MetadataValue.json(missing), "event_date": event_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {len(EXPECTED_CARRIERS)} carriers reported rate data for {event_date}.",
        metadata={"event_date": event_date},
    )
