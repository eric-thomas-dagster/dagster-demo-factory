"""Blocking check on the ABS pool eligibility mart.

Maps to the brief's stakes: SFS's 2026 capital strategy calls for up to eight
auto/lease ABS securitization deals, and loan-tape accuracy is under investor
and rating-agency scrutiny (SEC Reg AB). This check fails a partition whose
eligible_balance doesn't reconcile against the loan portfolio total minus
delinquent balances -- computed from the real synthesized marts, never
planted to fail.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["abs_pool_eligibility"]),
    blocking=True,
    description=(
        "Fails when eligible_balance doesn't reconcile against total_balance minus "
        "total_past_due for any region, or when a region reports a negative eligible_balance."
    ),
)
def abs_pool_eligibility_reconciliation(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key
    conn = connect_with_retry(demo_duckdb_path())
    try:
        rows = conn.execute(
            """
            select dealer_group, total_balance, total_past_due, eligible_balance
            from marts.abs_pool_eligibility
            where as_of_date = ?
            """,
            [event_date],
        ).fetchall()
    finally:
        conn.close()

    bad_regions = [
        dealer_group
        for dealer_group, total_balance, total_past_due, eligible_balance in rows
        if eligible_balance < 0 or abs(eligible_balance - (total_balance - total_past_due)) > 0.01
    ]

    if bad_regions:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{len(bad_regions)} of {len(rows)} regions for {event_date} have an eligible_balance that "
                f"doesn't reconcile to total_balance - total_past_due: {bad_regions}. This partition is not "
                "eligible for ABS pool reporting until it reconciles."
            ),
            metadata={"region_count": len(rows), "bad_regions": bad_regions, "as_of_date": event_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {len(rows)} regions for {event_date} reconcile: eligible_balance = total_balance - total_past_due.",
        metadata={"region_count": len(rows), "as_of_date": event_date},
    )
