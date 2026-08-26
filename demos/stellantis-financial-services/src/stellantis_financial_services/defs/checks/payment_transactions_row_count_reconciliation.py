"""Warning-severity bronze-to-silver reconciliation check (the brief's
optional fourth check). Maps to the pain quote "UI/visibility is
fragmented... hard to trust outputs" -- confirms no payment rows were
silently dropped (or duplicated) between the raw vendor file and the
conformed staging table.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey("stg_payment_transactions"),
    blocking=False,
    description="Warns when the staged payment transaction row count doesn't reconcile against the raw bronze feed for the day.",
)
def payment_transactions_row_count_reconciliation(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    payment_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        raw_count = conn.execute(
            "select count(*) from raw.payment_transactions where payment_date = ?", [payment_date]
        ).fetchone()[0]
        stg_count = conn.execute(
            "select count(*) from stg.payment_transactions where payment_date = ?", [payment_date]
        ).fetchone()[0]
    finally:
        conn.close()

    if raw_count != stg_count:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=(
                f"raw.payment_transactions has {raw_count} rows for {payment_date} but "
                f"stg.payment_transactions has {stg_count} -- worth confirming the dedup step didn't drop legitimate rows."
            ),
            metadata={"raw_row_count": raw_count, "staged_row_count": stg_count, "payment_date": payment_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"stg.payment_transactions reconciles 1:1 against raw.payment_transactions for {payment_date}: {stg_count} rows.",
        metadata={"raw_row_count": raw_count, "staged_row_count": stg_count, "payment_date": payment_date},
    )
