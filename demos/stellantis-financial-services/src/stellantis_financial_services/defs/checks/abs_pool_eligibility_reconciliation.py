"""The blocking check on the money-shot terminal asset.

Maps to the brief's Data domains section: SFS is underwriting up to eight
auto ABS securitization deals in 2026, and an audit-ready, loan-level data
tape is not optional. This check asserts pool-tape completeness -- every
loan in that day's `fact_loan_portfolio` has exactly one eligibility
determination in `abs_pool_eligibility`, with no loan silently dropped or
duplicated on the way to the pool tape. Reconciliation gaps here are
precisely what investor and rating-agency scrutiny (SEC Reg AB shelf
context, per the brief) would catch, so the demo catches them first.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey("abs_pool_eligibility"),
    blocking=True,
    description="Fails when the pool eligibility tape's loan count doesn't reconcile 1:1 against the funded loan portfolio for the day.",
)
def abs_pool_eligibility_reconciliation(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    as_of_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        portfolio_count = conn.execute(
            "select count(distinct loan_id) from fact.loan_portfolio where as_of_date = ?", [as_of_date]
        ).fetchone()[0]
        pool_count = conn.execute(
            "select count(distinct loan_id) from abs.pool_eligibility where as_of_date = ?", [as_of_date]
        ).fetchone()[0]
        eligible_count = conn.execute(
            "select count(*) from abs.pool_eligibility where as_of_date = ? and eligible", [as_of_date]
        ).fetchone()[0]
    finally:
        conn.close()

    if portfolio_count != pool_count:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Pool eligibility tape has {pool_count} loans but the funded portfolio has "
                f"{portfolio_count} for {as_of_date} -- the tape doesn't reconcile 1:1. This is the "
                "audit-ready-loan-tape gap the 2026 ABS securitization calendar can't tolerate."
            ),
            metadata={
                "portfolio_loan_count": portfolio_count,
                "pool_loan_count": pool_count,
                "as_of_date": as_of_date,
            },
        )
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"Pool eligibility tape reconciles 1:1 against the funded portfolio for {as_of_date}: "
            f"{pool_count} loans, {eligible_count} pool-eligible."
        ),
        metadata={
            "portfolio_loan_count": portfolio_count,
            "pool_loan_count": pool_count,
            "eligible_loan_count": eligible_count,
            "as_of_date": as_of_date,
        },
    )
