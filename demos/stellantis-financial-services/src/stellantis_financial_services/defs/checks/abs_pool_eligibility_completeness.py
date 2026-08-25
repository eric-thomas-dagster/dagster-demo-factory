"""The blocking check on the ABS pool eligibility mart.

Maps to SFS's 2026 capital strategy: up to eight auto/lease ABS
securitizations this year, with investor and rating-agency scrutiny of
loan-tape accuracy. `abs_pool_eligibility` computes a `pool_eligible` flag
per contract; this check refuses to certify a day's pool if any contract in
it is not eligible -- the pool cannot be treated as investor-ready with
incomplete or malformed loan-tape rows in it.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["marts", "abs_pool_eligibility"]),
    blocking=True,
    description="Fails when any contract for the day is not pool_eligible.",
)
def abs_pool_eligibility_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    contract_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        total, ineligible = conn.execute(
            "select count(*), sum(case when not pool_eligible then 1 else 0 end) "
            "from main_marts.abs_pool_eligibility where contract_date = ?",
            [contract_date],
        ).fetchone()
    finally:
        conn.close()
    ineligible = ineligible or 0

    if ineligible > 0:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{ineligible} of {total} contracts for {contract_date} are not pool_eligible. "
                "This day's pool is not ready for securitization reporting."
            ),
            metadata={"total_contracts": total, "ineligible_contracts": ineligible, "contract_date": contract_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {total} contracts for {contract_date} are pool_eligible.",
        metadata={"total_contracts": total, "contract_date": contract_date},
    )
