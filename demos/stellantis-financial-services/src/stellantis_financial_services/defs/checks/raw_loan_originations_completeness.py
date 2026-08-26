"""The blocking check on the loan origination bronze feed.

Maps to the brief's Pain quote: "failure recovery is manual, replay is
weak." A loan record missing its id, dealer, or amount -- or carrying a
negative/zero amount -- structurally cannot reach `stg_loan_originations`
and everything downstream of it (the fact table, the ABS pool tape), because
this check gates the pipeline trigger before anything downstream runs. The
prospect doesn't need to watch it fail to believe that -- the check's config
and its passing result are both visible in the UI, against a green graph.

Registry search before writing this (per CLAUDE.md's asset-check mandate):
`"sql assertion check"` (no hits), `"data quality check"` (surfaced
`enhanced_data_quality_checks` -- a generic DataFrame column-stats library;
doesn't fit a partition-scoped SQL gate against a specific business rule
spanning three columns), `"row count reconciliation check"` (no hits). Native
`@dg.asset_check` is core Dagster, not a registry rung -- this follows the
same pattern as every other check in this repo (see kapitus, iso-new-england).
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey("raw_loan_originations"),
    blocking=True,
    description=(
        "Fails when any loan record is missing loan_id, dealer_id, or borrower_id, "
        "or has a non-positive loan_amount."
    ),
)
def raw_loan_originations_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    origination_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        bad_count = conn.execute(
            """
            select count(*) from raw.loan_originations
            where origination_date = ?
              and (loan_id is null or dealer_id is null or borrower_id is null or loan_amount <= 0)
            """,
            [origination_date],
        ).fetchone()[0]
        row_count = conn.execute(
            "select count(*) from raw.loan_originations where origination_date = ?",
            [origination_date],
        ).fetchone()[0]
    finally:
        conn.close()

    if bad_count > 0:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{bad_count} of {row_count} loan records for {origination_date} are missing a "
                "required field or have a non-positive loan_amount. stg_loan_originations and every "
                "downstream mart are blocked for this partition, not computed on a corrupted record."
            ),
            metadata={"row_count": row_count, "bad_count": bad_count, "origination_date": origination_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {row_count} loan records for {origination_date} have the required fields and a sane amount.",
        metadata={"row_count": row_count, "origination_date": origination_date},
    )
