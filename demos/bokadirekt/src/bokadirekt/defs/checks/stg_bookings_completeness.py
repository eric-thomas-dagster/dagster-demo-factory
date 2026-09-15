"""Blocking completeness check on the bookings staging layer.

Direct reversal of the brief's own framing: Noel's stuck workflow has no
way to know if a merge to prod is actually safe to build on top of; this
check is the concrete answer -- if the bookings feed comes back empty or
with unidentifiable rows, the entity layer refuses to compute on it,
instead of silently rolling forward the way a hand-debugged deploy might.

No registry component covers a declarative completeness check against an
arbitrary DuckDB query -- asset-check assertion logic is business logic,
one of the justified `.py` files in `defs/checks/` (see README).
"""

import dagster as dg

from bokadirekt.demo_data.warehouse import demo_duckdb_path

MINIMUM_EXPECTED_ROWS = 20


@dg.asset_check(
    asset=dg.AssetKey(["staging", "stg_bookings"]),
    blocking=True,
    description=(
        f"Fails when fewer than {MINIMUM_EXPECTED_ROWS} bookings landed for "
        "the day, or any booking_id is null -- blocks the entity layer from "
        "computing on an incomplete or malformed feed."
    ),
)
def stg_bookings_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        row_count, null_id_count = conn.execute(
            "select count(*), count(*) filter (where booking_id is null) from main_staging.stg_bookings"
        ).fetchone()
    finally:
        conn.close()

    passed = row_count >= MINIMUM_EXPECTED_ROWS and null_id_count == 0
    return dg.AssetCheckResult(
        passed=passed,
        description=(
            f"{row_count} bookings on file ({null_id_count} with a null booking_id) "
            f"against a floor of {MINIMUM_EXPECTED_ROWS}."
        ),
        metadata={
            "row_count": row_count,
            "null_booking_id_count": null_id_count,
            "minimum_expected_rows": MINIMUM_EXPECTED_ROWS,
        },
    )
