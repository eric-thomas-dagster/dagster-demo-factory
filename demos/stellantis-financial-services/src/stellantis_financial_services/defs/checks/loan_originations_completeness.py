"""Blocking check: raw_loan_originations required-field completeness.

Maps to the AE's own words: "failure recovery is manual, replay is weak."
Bad data structurally cannot reach `stg_loan_originations` -- the check
gates the Fabric pipeline trigger before anything downstream runs.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["raw", "raw_loan_originations"]),
    blocking=True,
    description="Fails if any loan is missing loan_id, dealer_id, or a positive principal_amount.",
)
def raw_loan_originations_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key
    conn = connect_with_retry(demo_duckdb_path())
    try:
        bad_rows = conn.execute(
            """
            select count(*) from raw.loan_originations
            where origination_date = ?
              and (loan_id is null or dealer_id is null or principal_amount is null or principal_amount <= 0)
            """,
            [event_date],
        ).fetchone()[0]
        total_rows = conn.execute(
            "select count(*) from raw.loan_originations where origination_date = ?", [event_date]
        ).fetchone()[0]
    finally:
        conn.close()

    passed = bad_rows == 0
    return dg.AssetCheckResult(
        passed=passed,
        description=(
            f"{bad_rows} of {total_rows} loans missing loan_id/dealer_id/principal_amount for {event_date}."
            if not passed
            else f"All {total_rows} loans for {event_date} have loan_id, dealer_id, and a positive principal_amount."
        ),
        metadata={"bad_rows": bad_rows, "total_rows": total_rows, "event_date": event_date},
    )
