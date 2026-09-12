"""Blocking completeness check on the battery-materials data product that
ultimately feeds EU Battery Regulation carbon-footprint reporting.

Directly extends the "revenue and reporting risk avoided" argument already
in the AE's own business case: mandatory carbon-footprint declarations for
EV batteries (since Feb 2025) and rechargeable industrial batteries above
2kWh (from Feb 2026) require traceable, auditable data. A day with null or
non-positive production totals means the upstream battery-materials feed
landed incomplete -- `eu_battery_regulation_carbon_footprint_report` and
everything reading `fct_battery_carbon_footprint_inputs` shouldn't silently
compute (or, worse, report) on top of a partial day.

No registry component covers a declarative completeness check against an
arbitrary DuckDB query -- asset-check assertion logic is business logic,
one of the justified `.py` files in `defs/checks/` (see README).
"""

import dagster as dg

from umicore.demo_data.warehouse import demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["marts", "fct_battery_carbon_footprint_inputs"]),
    blocking=True,
    description=(
        "Fails when any daily row has a null or non-positive total_output_kg -- blocks "
        "eu_battery_regulation_carbon_footprint_report from computing on incomplete "
        "battery-materials production inputs."
    ),
)
def fct_battery_carbon_footprint_inputs_completeness(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        row_count, bad_rows = conn.execute(
            "select count(*), "
            "sum(case when total_output_kg is null or total_output_kg <= 0 then 1 else 0 end) "
            "from main_marts.fct_battery_carbon_footprint_inputs"
        ).fetchone()
    finally:
        conn.close()

    bad_rows = bad_rows or 0
    passed = row_count > 0 and bad_rows == 0
    return dg.AssetCheckResult(
        passed=passed,
        description=(
            f"{row_count} daily rows, {bad_rows} with null/non-positive output_kg."
            if passed
            else f"{bad_rows} of {row_count} daily rows have a null or non-positive "
            "total_output_kg -- battery-materials production inputs look incomplete."
        ),
        metadata={"row_count": row_count, "incomplete_rows": bad_rows},
    )
