"""Warning row-count/schema check on the AC (Automotive Catalysts) daily
feed -- standing in for the "at least one team's workflow is down on any
given day," and specifically the AC unit's one-full-time-engineer,
~2-hours-a-day manual log-checking cost named explicitly in the business
case. Demonstrates the manual-monitoring pain being replaced *without*
implying the feed was silently corrupting data before -- it wasn't; it was
just unwatched. Warning severity: this is a heads-up for a human, not a
gate on downstream compute (the brief reserves blocking severity for the
carbon-footprint and 3-job-chain checks).

No registry component covers a declarative row-count/schema check against
an arbitrary DuckDB query -- asset-check assertion logic is business logic,
one of the justified `.py` files in `defs/checks/` (see README).
"""

import dagster as dg

from umicore.demo_data.warehouse import demo_duckdb_path

EXPECTED_COLUMNS = {
    "feed_record_id",
    "catalyst_unit_id",
    "conversion_rate_pct",
    "throughput_units",
    "recorded_on",
}
MINIMUM_EXPECTED_ROWS = 20


@dg.asset_check(
    asset=dg.AssetKey(["catalysis", "raw_ac_daily_feed"]),
    blocking=False,
    description=(
        "Warns when the AC daily feed's row count looks low or its columns have drifted "
        "-- the automated stand-in for the AC team's daily manual log check."
    ),
)
def raw_ac_daily_feed_quality(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        row_count = conn.execute("select count(*) from raw.ac_daily_feed").fetchone()[0]
        columns = {
            row[0]
            for row in conn.execute(
                "select column_name from information_schema.columns where table_schema = 'raw' "
                "and table_name = 'ac_daily_feed'"
            ).fetchall()
        }
    finally:
        conn.close()

    missing_columns = EXPECTED_COLUMNS - columns
    low_row_count = row_count < MINIMUM_EXPECTED_ROWS
    passed = not missing_columns and not low_row_count

    if passed:
        description = f"{row_count} AC feed rows on file with all expected columns present."
    elif missing_columns:
        description = f"AC feed is missing expected columns: {sorted(missing_columns)}."
    else:
        description = (
            f"Only {row_count} AC feed rows on file against a floor of {MINIMUM_EXPECTED_ROWS} "
            "-- worth a look before the AC team's own daily check would catch it."
        )

    return dg.AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "row_count": row_count,
            "minimum_expected_rows": MINIMUM_EXPECTED_ROWS,
            "missing_columns": sorted(missing_columns),
        },
    )
