"""Blocking completeness check on the daily loan origination feed.

Same pain as `raw_member_transactions_completeness` (see that module's
docstring): Dagster refusing to compute on an incomplete upstream feed,
instead of a hand-built job-ordering workaround silently running anyway.
Loans originate far less often than member transactions, so this floor is
much lower -- a single missing day is still meaningful for the loan
portfolio, unlike a quiet hour of transactions.
"""

import dagster as dg

from partners_fcu.demo_data.warehouse import demo_duckdb_path

MINIMUM_EXPECTED_ROWS = 5


@dg.asset_check(
    asset=dg.AssetKey("raw_loan_originations"),
    blocking=True,
    description=(
        f"Fails when fewer than {MINIMUM_EXPECTED_ROWS} loans originated for the "
        "day -- blocks stg_loan_originations and the loan portfolio summary from "
        "computing on an incomplete daily feed."
    ),
)
def raw_loan_originations_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        row_count = conn.execute("select count(*) from raw.raw_loan_originations").fetchone()[0]
    finally:
        conn.close()

    if row_count < MINIMUM_EXPECTED_ROWS:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Only {row_count} loan originations on file against a floor of "
                f"{MINIMUM_EXPECTED_ROWS} -- the daily feed looks incomplete. "
                "stg_loan_originations and fct_loan_portfolio_summary are blocked "
                "for this run."
            ),
            metadata={"row_count": row_count, "minimum_expected_rows": MINIMUM_EXPECTED_ROWS},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"{row_count} loan originations on file, above the floor of {MINIMUM_EXPECTED_ROWS}.",
        metadata={"row_count": row_count, "minimum_expected_rows": MINIMUM_EXPECTED_ROWS},
    )
