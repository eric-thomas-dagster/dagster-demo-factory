"""The blocking check on the loan origination bronze feed.

Maps to the brief's AE quote: "failure recovery is manual, replay is weak" --
bad loan data structurally cannot reach `stg_loan_originations` and every
downstream mart, because this check gates it before anything downstream runs.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path

_REQUIRED_COLUMNS = ("loan_id", "amount_financed", "dealer_id")


@dg.asset_check(
    asset=dg.AssetKey(["raw", "loan_originations"]),
    blocking=True,
    description="Fails when any row is missing loan_id, amount_financed, or dealer_id.",
)
def loan_originations_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    contract_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        null_counts = {}
        for column in _REQUIRED_COLUMNS:
            null_counts[column] = conn.execute(
                f"select count(*) from raw.loan_originations where contract_date = ? and {column} is null",
                [contract_date],
            ).fetchone()[0]
        row_count = conn.execute(
            "select count(*) from raw.loan_originations where contract_date = ?", [contract_date]
        ).fetchone()[0]
    finally:
        conn.close()

    total_nulls = sum(null_counts.values())
    if total_nulls > 0:
        offending = ", ".join(f"{col}={count}" for col, count in null_counts.items() if count > 0)
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{total_nulls} required-field nulls in {row_count} rows for {contract_date} ({offending}). "
                "Downstream staging and marts are blocked for this partition, not computed on incomplete data."
            ),
            metadata={"row_count": row_count, "null_field_counts": null_counts, "contract_date": contract_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {row_count} rows for {contract_date} have complete required fields.",
        metadata={"row_count": row_count, "contract_date": contract_date},
    )
