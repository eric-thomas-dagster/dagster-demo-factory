"""Warning check: bronze-to-silver row-count reconciliation on payments.

Maps to the AE's "UI/visibility is fragmented... hard to trust outputs" pain.
Warning, not blocking -- a conforming pass that drops rows (bad payment
method codes, malformed contract references) is worth a flag, not a halt;
unlike the other two blocking checks, nothing downstream is unsafe to
compute on a small, expected amount of drop.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["staging", "stg_payment_transactions"]),
    blocking=False,
    description="Warns if stg_payment_transactions has fewer rows than raw_payment_transactions for the same day.",
)
def payment_transactions_reconciliation(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key
    conn = connect_with_retry(demo_duckdb_path())
    try:
        raw_count = conn.execute(
            "select count(*) from raw.payment_transactions where payment_date = ?", [event_date]
        ).fetchone()[0]
        staged_count = conn.execute(
            "select count(*) from staging.stg_payment_transactions where payment_date = ?", [event_date]
        ).fetchone()[0]
    finally:
        conn.close()

    passed = staged_count >= raw_count
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"stg_payment_transactions has {staged_count} rows vs. {raw_count} in raw for {event_date}."
            if not passed
            else f"stg_payment_transactions reconciles with raw ({staged_count} rows) for {event_date}."
        ),
        metadata={"raw_count": raw_count, "staged_count": staged_count, "event_date": event_date},
    )
