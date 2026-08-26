"""Blocking check: abs_pool_eligibility reconciles against the loan tape it was built from.

Maps to SFS's 2026 ABS securitization calendar (up to eight deals) and the
investor/rating-agency scrutiny of loan-tape accuracy that comes with it.
Every contract in `fact_loan_portfolio` for the day must show up exactly
once in `abs_pool_eligibility`, and every eligible contract must carry a
credit score -- a pool can't be represented to investors as evaluated
against criteria it was never actually checked against.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["marts", "abs_pool_eligibility"]),
    blocking=True,
    description="Fails if the pool row count doesn't reconcile 1:1 with fact_loan_portfolio, or an eligible contract lacks a credit score.",
)
def abs_pool_eligibility_reconciliation(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key
    conn = connect_with_retry(demo_duckdb_path())
    try:
        portfolio_count, pool_count = conn.execute(
            """
            select
                (select count(*) from marts.fact_loan_portfolio where origination_date = ?),
                (select count(*) from marts.abs_pool_eligibility where origination_date = ?)
            """,
            [event_date, event_date],
        ).fetchone()
        eligible_missing_score = conn.execute(
            """
            select count(*) from marts.abs_pool_eligibility
            where origination_date = ? and is_pool_eligible and credit_score is null
            """,
            [event_date],
        ).fetchone()[0]
    finally:
        conn.close()

    reconciles = portfolio_count == pool_count
    passed = reconciles and eligible_missing_score == 0
    if not reconciles:
        description = f"Pool has {pool_count} contracts but the loan tape has {portfolio_count} for {event_date}."
    elif eligible_missing_score:
        description = f"{eligible_missing_score} contracts marked pool-eligible are missing a credit score."
    else:
        description = f"Pool reconciles 1:1 with the loan tape ({pool_count} contracts), all eligible contracts scored."

    return dg.AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "portfolio_count": portfolio_count,
            "pool_count": pool_count,
            "eligible_missing_score": eligible_missing_score,
            "event_date": event_date,
        },
    )
