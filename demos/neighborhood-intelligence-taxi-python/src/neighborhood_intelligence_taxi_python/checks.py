"""Asset checks on `fct_trips` -- native Dagster equivalents of the parent
project's dbt-generated tests.

Three checks, all blocking severity:

- `fct_trips_trip_id_not_null` -- equivalent to dbt's
  `not_null_fct_trips_trip_id` generic test.
- `fct_trips_trip_id_unique` -- equivalent to dbt's
  `unique_fct_trips_trip_id` generic test.
- `fct_trips_reconciliation` -- row-count parity between silver's
  `stg_yellow_trips` and gold's `fct_trips`. Equivalent to dbt's
  `equal_rowcount_fct_trips_ref_stg_yellow_trips_` generic test. Catches
  the zone left-join silently dropping rows or a broken trip_id key.

Every one is ~15 lines of Python where dbt+DCC would be 2-6 lines of YAML.
"""

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    asset_check,
)
from dagster_duckdb import DuckDBResource


@asset_check(
    asset=AssetKey(["gold", "fct_trips"]),
    blocking=True,
    description="fct_trips.trip_id has no NULLs -- blocking natural-key contract.",
)
def fct_trips_trip_id_not_null(
    context: AssetCheckExecutionContext, duckdb: DuckDBResource
) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        null_count = conn.execute(
            "SELECT COUNT(*) FROM main_gold.fct_trips WHERE trip_id IS NULL"
        ).fetchone()[0]
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_count": int(null_count)},
        description=f"fct_trips.trip_id null count: {null_count}",
    )


@asset_check(
    asset=AssetKey(["gold", "fct_trips"]),
    blocking=True,
    description="fct_trips.trip_id is unique -- blocking natural-key contract.",
)
def fct_trips_trip_id_unique(
    context: AssetCheckExecutionContext, duckdb: DuckDBResource
) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        duplicate_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT trip_id
                FROM main_gold.fct_trips
                GROUP BY trip_id
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    return AssetCheckResult(
        passed=duplicate_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_trip_id_count": int(duplicate_count)},
        description=f"fct_trips.trip_id duplicates: {duplicate_count}",
    )


@asset_check(
    asset=AssetKey(["gold", "fct_trips"]),
    additional_deps=[AssetKey(["silver", "stg_yellow_trips"])],
    blocking=True,
    description=(
        "Row-count reconciliation: fct_trips must equal stg_yellow_trips. "
        "Catches the zone left-join silently dropping rows or a broken "
        "trip_id key."
    ),
)
def fct_trips_reconciliation(
    context: AssetCheckExecutionContext, duckdb: DuckDBResource
) -> AssetCheckResult:
    with duckdb.get_connection() as conn:
        silver_count = conn.execute(
            "SELECT COUNT(*) FROM main_silver.stg_yellow_trips"
        ).fetchone()[0]
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM main_gold.fct_trips"
        ).fetchone()[0]
    silver, gold = int(silver_count), int(gold_count)
    return AssetCheckResult(
        passed=silver == gold,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "silver_rows": silver,
            "gold_rows": gold,
            "diff": silver - gold,
        },
        description=(
            f"fct_trips={gold} vs stg_yellow_trips={silver}"
            + ("" if silver == gold else f" (diff {silver - gold})")
        ),
    )
