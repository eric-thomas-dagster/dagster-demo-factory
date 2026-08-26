"""The blocking check on the loan application bronze feed.

Maps to the brief's build directive: "funded amount must be non-negative and
within a sane bound; nothing downstream computes on a corrupted funding
record" -- answers the pain of "no unified data quality control." A bad
funding record structurally cannot reach `funded_loans_daily` and every
downstream mart, because this check gates it before anything downstream runs.
"""

import dagster as dg

from kapitus.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["raw", "loan_applications"]),
    blocking=True,
    description="Fails when any funded application has a negative funded_amount or a funded_amount above its requested_amount.",
)
def loan_applications_funded_amount_sanity(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    partition_key = context.partition_key
    application_date = partition_key.keys_by_dimension["date"]
    product_line = partition_key.keys_by_dimension["product_line"]

    conn = connect_with_retry(demo_duckdb_path())
    try:
        bad_count = conn.execute(
            """
            select count(*) from raw.loan_applications
            where application_date = ? and product_line = ?
              and funding_status = 'funded'
              and (funded_amount < 0 or funded_amount > requested_amount)
            """,
            [application_date, product_line],
        ).fetchone()[0]
        row_count = conn.execute(
            "select count(*) from raw.loan_applications where application_date = ? and product_line = ?",
            [application_date, product_line],
        ).fetchone()[0]
    finally:
        conn.close()

    if bad_count > 0:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{bad_count} of {row_count} applications for {product_line} on {application_date} have a "
                "funded_amount outside [0, requested_amount]. funded_loans_daily and every downstream mart "
                "are blocked for this partition, not computed on a corrupted funding record."
            ),
            metadata={"row_count": row_count, "bad_count": bad_count, "application_date": application_date, "product_line": product_line},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {row_count} applications for {product_line} on {application_date} have a sane funded_amount.",
        metadata={"row_count": row_count, "application_date": application_date, "product_line": product_line},
    )
