"""Blocking check on the loan origination bronze feed.

Maps to the brief's pain: *"failure recovery is manual, replay is weak."* A
loan record missing its loan_id, dealer_id, or amount structurally cannot
reach `stg_loan_originations` -- and every downstream mart, including
`abs_pool_eligibility` -- because this check gates the Fabric pipeline
trigger before anything downstream runs. It computes from the real synthetic
data (never planted to fail) so the pass/fail condition is genuine, not
asserted.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["raw_loan_originations"]),
    blocking=True,
    description=(
        "Fails when any loan origination row is missing loan_id, dealer_id, or "
        "principal_amount before stg_loan_originations is triggered."
    ),
)
def raw_loan_originations_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key
    conn = connect_with_retry(demo_duckdb_path())
    try:
        row_count = conn.execute(
            "select count(*) from raw.raw_loan_originations where origination_date = ?", [event_date]
        ).fetchone()[0]
        bad_count = conn.execute(
            """
            select count(*) from raw.raw_loan_originations
            where origination_date = ?
              and (loan_id is null or dealer_id is null or principal_amount is null)
            """,
            [event_date],
        ).fetchone()[0]
    finally:
        conn.close()

    if bad_count > 0:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{bad_count} of {row_count} loan originations for {event_date} are missing a required "
                "field. stg_loan_originations and every downstream mart are blocked for this partition, "
                "not computed on an incomplete origination record."
            ),
            metadata={"row_count": row_count, "bad_count": bad_count, "origination_date": event_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {row_count} loan originations for {event_date} have loan_id, dealer_id, and principal_amount.",
        metadata={"row_count": row_count, "origination_date": event_date},
    )
