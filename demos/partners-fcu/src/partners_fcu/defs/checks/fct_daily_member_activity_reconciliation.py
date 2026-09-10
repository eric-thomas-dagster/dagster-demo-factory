"""Warning-severity reconciliation check on the daily member activity fact.

Same pain as the two completeness checks (no way to know if data output
was good, only whether the job ran), softer signal: this compares the
fact table's total transaction count against the raw ingested count to
catch the aggregation step silently dropping or duplicating rows, rather
than a whole day's feed going quiet.
"""

import dagster as dg

from partners_fcu.demo_data.warehouse import demo_duckdb_path

RECONCILIATION_TOLERANCE = 0


@dg.asset_check(
    asset=dg.AssetKey(["marts", "fct_daily_member_activity"]),
    blocking=False,
    description=(
        "Warns when the sum of transaction_count in fct_daily_member_activity "
        "doesn't match the raw member-transactions row count -- would catch the "
        "aggregation step silently dropping or duplicating rows."
    ),
)
def fct_daily_member_activity_reconciliation(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        raw_count = conn.execute("select count(*) from raw.raw_member_transactions").fetchone()[0]
        fact_total = conn.execute(
            "select coalesce(sum(transaction_count), 0) from main_marts.fct_daily_member_activity"
        ).fetchone()[0]
    finally:
        conn.close()

    diff = abs(raw_count - fact_total)
    if diff > RECONCILIATION_TOLERANCE:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=(
                f"fct_daily_member_activity totals {fact_total} transactions against "
                f"{raw_count} raw member-transaction rows -- a difference of {diff}."
            ),
            metadata={"raw_count": raw_count, "fact_total": fact_total, "difference": diff},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"fct_daily_member_activity reconciles exactly against {raw_count} raw member-transaction rows.",
        metadata={"raw_count": raw_count, "fact_total": fact_total},
    )
