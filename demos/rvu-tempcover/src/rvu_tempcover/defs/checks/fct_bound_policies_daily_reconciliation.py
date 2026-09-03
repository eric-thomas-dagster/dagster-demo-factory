"""Warning-severity reconciliation check on the bound-policies fact.

Same pain as the panel completeness check (no way to know if data output
was good), softer signal: this compares the fact table's total policy
count against the raw ingested count to catch the aggregation step
silently dropping rows, rather than a whole source going quiet.
"""

import dagster as dg

from rvu_tempcover.demo_data.warehouse import demo_duckdb_path

RECONCILIATION_TOLERANCE = 0


@dg.asset_check(
    asset=dg.AssetKey(["marts", "fct_bound_policies_daily"]),
    blocking=False,
    description=(
        "Warns when the sum of policy_count in fct_bound_policies_daily doesn't "
        "match the raw bound-policies row count -- would catch the aggregation "
        "step silently dropping or duplicating rows."
    ),
)
def fct_bound_policies_daily_reconciliation(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        raw_count = conn.execute("select count(*) from raw.bound_policies").fetchone()[0]
        fact_total = conn.execute(
            "select coalesce(sum(policy_count), 0) from main_marts.fct_bound_policies_daily"
        ).fetchone()[0]
    finally:
        conn.close()

    diff = abs(raw_count - fact_total)
    if diff > RECONCILIATION_TOLERANCE:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=(
                f"fct_bound_policies_daily totals {fact_total} policies against "
                f"{raw_count} raw bound_policies rows -- a difference of {diff}."
            ),
            metadata={"raw_count": raw_count, "fact_total": fact_total, "difference": diff},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"fct_bound_policies_daily reconciles exactly against {raw_count} raw bound_policies rows.",
        metadata={"raw_count": raw_count, "fact_total": fact_total},
    )
