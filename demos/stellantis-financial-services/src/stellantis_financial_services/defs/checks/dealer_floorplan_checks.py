"""The two checks on the dealer floorplan bronze feed.

The brief names a blocking completeness check on `raw_loan_originations` and
a warning lateness check on `raw_dealer_floorplan_feed`, but also describes
the planted anomaly as a *malformed record* on `raw_dealer_floorplan_feed`
that must block downstream computation for that partition -- which needs a
blocking check on that same asset, not just a warning one. Both checks live
here: `dealer_floorplan_completeness` (blocking, catches the planted
anomaly and drives the money-shot recovery demo) and `dealer_floorplan_lateness`
(warning, the arrival-time/lateness visibility the brief asks for).
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path

_REQUIRED_COLUMNS = ("floorplan_advance_id", "dealer_id", "vehicle_vin", "advance_amount")
_MIN_EXPECTED_ROWS = 10


@dg.asset_check(
    asset=dg.AssetKey(["raw", "dealer_floorplan_feed"]),
    blocking=True,
    description="Fails when any advance record is missing floorplan_advance_id, dealer_id, vehicle_vin, or advance_amount.",
)
def dealer_floorplan_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    advance_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        null_counts = {}
        for column in _REQUIRED_COLUMNS:
            null_counts[column] = conn.execute(
                f"select count(*) from raw.dealer_floorplan_feed where advance_date = ? and {column} is null",
                [advance_date],
            ).fetchone()[0]
        row_count = conn.execute(
            "select count(*) from raw.dealer_floorplan_feed where advance_date = ?", [advance_date]
        ).fetchone()[0]
    finally:
        conn.close()

    total_nulls = sum(null_counts.values())
    if total_nulls > 0:
        offending = ", ".join(f"{col}={count}" for col, count in null_counts.items() if count > 0)
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{total_nulls} required-field nulls in {row_count} rows for {advance_date} ({offending}). "
                "dim_dealer and every downstream mart are blocked for this partition until the dealer "
                "resends a corrected file."
            ),
            metadata={"row_count": row_count, "null_field_counts": null_counts, "advance_date": advance_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {row_count} advance records for {advance_date} have complete required fields.",
        metadata={"row_count": row_count, "advance_date": advance_date},
    )


@dg.asset_check(
    asset=dg.AssetKey(["raw", "dealer_floorplan_feed"]),
    blocking=False,
    description=f"Warns when a day's batch has fewer than {_MIN_EXPECTED_ROWS} advance records -- a proxy for a feed that arrived unusually thin or late.",
)
def dealer_floorplan_lateness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    advance_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        row_count = conn.execute(
            "select count(*) from raw.dealer_floorplan_feed where advance_date = ?", [advance_date]
        ).fetchone()[0]
    finally:
        conn.close()

    if row_count < _MIN_EXPECTED_ROWS:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=(
                f"Only {row_count} advance records for {advance_date}, expected at least "
                f"{_MIN_EXPECTED_ROWS} -- worth confirming the dealer's batch arrived complete."
            ),
            metadata={"row_count": row_count, "advance_date": advance_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"{row_count} advance records for {advance_date}, within expected volume.",
        metadata={"row_count": row_count, "advance_date": advance_date},
    )
