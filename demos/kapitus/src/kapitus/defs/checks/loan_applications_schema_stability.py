"""Schema-change check on the Fivetran-sourced loan application asset.

Directly the AE's POC test criterion: "schema-change alerts." Fivetran syncs
can silently add or drop columns on a schema change upstream; this check
compares the raw table's actual columns against the expected contract every
run so a drift surfaces here rather than as a silent break several models
downstream. Warning severity -- a schema change is worth a human look, not an
automatic halt of the whole chain the way a corrupted funding record is.
"""

import dagster as dg

from kapitus.demo_data.warehouse import connect_with_retry, demo_duckdb_path

_EXPECTED_COLUMNS = {
    "application_id", "application_date", "product_line", "business_id", "business_state",
    "requested_amount", "funding_status", "funded_amount", "apr", "term_months",
}


@dg.asset_check(
    asset=dg.AssetKey(["raw", "loan_applications"]),
    blocking=False,
    description="Warns when the Fivetran-synced loan_applications table's columns drift from the expected contract.",
)
def loan_applications_schema_stability(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    conn = connect_with_retry(demo_duckdb_path())
    try:
        actual_columns = {
            row[0]
            for row in conn.execute(
                "select column_name from information_schema.columns where table_schema = 'raw' and table_name = 'loan_applications'"
            ).fetchall()
        }
    finally:
        conn.close()

    missing = _EXPECTED_COLUMNS - actual_columns
    added = actual_columns - _EXPECTED_COLUMNS
    if missing or added:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=(
                f"loan_applications schema drifted from the expected contract -- missing: {sorted(missing) or 'none'}, "
                f"added: {sorted(added) or 'none'}. Worth confirming this reflects a real Fivetran connector schema change."
            ),
            metadata={"missing_columns": sorted(missing), "added_columns": sorted(added)},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"loan_applications has all {len(_EXPECTED_COLUMNS)} expected columns, no drift.",
        metadata={"column_count": len(actual_columns)},
    )
