"""Completeness check on the credit bureau bronze feed.

Maps to the brief's underwriting-risk pain: a bureau pull missing either
score can't support a funding decision. Warning severity -- unlike a
corrupted funded_amount, a missing score on one applicant shouldn't halt the
whole day's chain, but it is worth surfacing before credit_risk_summary
quietly averages over a gap.
"""

import dagster as dg

from kapitus.demo_data.warehouse import connect_with_retry, demo_duckdb_path

_REQUIRED_COLUMNS = ("business_credit_score", "personal_credit_score")


@dg.asset_check(
    asset=dg.AssetKey(["raw", "credit_bureau_pulls"]),
    blocking=False,
    description="Warns when any bureau pull is missing business_credit_score or personal_credit_score.",
)
def credit_bureau_pull_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    partition_key = context.partition_key
    pull_date = partition_key.keys_by_dimension["date"]
    product_line = partition_key.keys_by_dimension["product_line"]

    conn = connect_with_retry(demo_duckdb_path())
    try:
        null_counts = {}
        for column in _REQUIRED_COLUMNS:
            null_counts[column] = conn.execute(
                f"select count(*) from raw.credit_bureau_pulls where pull_date = ? and product_line = ? and {column} is null",
                [pull_date, product_line],
            ).fetchone()[0]
        row_count = conn.execute(
            "select count(*) from raw.credit_bureau_pulls where pull_date = ? and product_line = ?",
            [pull_date, product_line],
        ).fetchone()[0]
    finally:
        conn.close()

    total_nulls = sum(null_counts.values())
    if total_nulls > 0:
        offending = ", ".join(f"{col}={count}" for col, count in null_counts.items() if count > 0)
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=(
                f"{total_nulls} missing scores in {row_count} pulls for {product_line} on {pull_date} ({offending})."
            ),
            metadata={"row_count": row_count, "null_field_counts": null_counts, "pull_date": pull_date, "product_line": product_line},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {row_count} bureau pulls for {product_line} on {pull_date} have both scores.",
        metadata={"row_count": row_count, "pull_date": pull_date, "product_line": product_line},
    )
