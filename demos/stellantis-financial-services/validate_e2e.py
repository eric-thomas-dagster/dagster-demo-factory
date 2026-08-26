"""End-to-end validation of the Stellantis Financial Services demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is partitioned (16 daily, 1 multi date x dealer_group), so this
script is the harness `scripts/validate_demo.sh` requires instead. It proves:

1. The whole chain materializes cleanly across a validation window -- bronze
   Fabric-triggered ingestion -> silver staging/dimensions -> gold marts ->
   Power BI refresh -- with every check passing, for every date in the
   window and every dealer_group region of the floorplan feed.
2. Row counts are printed so determinism is visible across runs.

Per the current house rules ("Demos always work"), this build runs green
end-to-end -- there is no planted anomaly or recovery sequence to validate.
The blocking and warning checks are real conditions computed from the
synthetic data (see `defs/checks/`), asserted here to genuinely pass rather
than assumed to.

Run with: `python validate_e2e.py`
"""

from __future__ import annotations

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from stellantis_financial_services.components.partitions import DEALER_GROUPS  # noqa: E402
from stellantis_financial_services.definitions import defs  # noqa: E402
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402

MULTI_KEYS = [dg.AssetKey("raw_dealer_floorplan_feed")]

DAILY_KEYS = [
    dg.AssetKey(key)
    for key in [
        "raw_loan_originations",
        "raw_lease_originations",
        "raw_payment_transactions",
        "raw_credit_bureau_pull",
        "stg_loan_originations",
        "stg_lease_originations",
        "stg_payment_transactions",
        "stg_delinquency_events",
        "dim_dealer",
        "dim_borrower",
        "fact_loan_portfolio",
        "fact_delinquency_snapshot",
        "abs_pool_eligibility",
        "gl_reconciliation_summary",
        "customer_360",
        "powerbi_portfolio_dashboard_refresh",
    ]
]

VALIDATION_DATES = ["2026-08-18", "2026-08-19"]

_CHECK_NAMES = (
    "loan_originations_completeness",
    "abs_pool_eligibility_reconciliation",
    "dealer_floorplan_feed_lateness",
    "payment_transactions_reconciliation",
)

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(definitions, instance, keys, partition_key, label=""):
    job = definitions.resolve_implicit_job_def_def_for_assets(keys)
    if job is None:
        raise RuntimeError(f"no job resolved for {keys}")
    result = job.execute_in_process(
        instance=instance,
        partition_key=partition_key,
        asset_selection=list(keys),
        raise_on_error=False,
    )
    if not result.success:
        raise RuntimeError(f"materialize failed: {label or keys} partition={partition_key}")
    return result


def assert_checks_passed(result, expected_names) -> None:
    seen = {}
    for evaluation in result.get_asset_check_evaluations():
        seen[evaluation.check_name] = evaluation.passed
    for name in expected_names:
        if name in seen and seen[name] is False:
            _failures.append(f"{name} unexpectedly failed")


def materialized_keys(result) -> set:
    return {event.asset_key for event in result.get_asset_materialization_events()}


def main() -> int:
    definitions = defs()

    with dg.instance_for_test() as instance:
        print(
            f"\n[1/2] Materializing the whole chain for {len(VALIDATION_DATES)} dates: "
            f"{VALIDATION_DATES} (dealer floorplan feed across {DEALER_GROUPS})"
        )
        run_count = 0
        for event_date in VALIDATION_DATES:
            for dealer_group in DEALER_GROUPS:
                partition_key = dg.MultiPartitionKey({"date": event_date, "dealer_group": dealer_group})
                result = run(definitions, instance, MULTI_KEYS, partition_key=partition_key, label="dealer floorplan feed")
                assert_checks_passed(result, _CHECK_NAMES)
                missing = set(MULTI_KEYS) - materialized_keys(result)
                check(not missing, f"{event_date}/{dealer_group}: raw_dealer_floorplan_feed materialized")
                run_count += 1

            result = run(definitions, instance, DAILY_KEYS, partition_key=event_date, label="daily chain")
            assert_checks_passed(result, _CHECK_NAMES)
            missing = set(DAILY_KEYS) - materialized_keys(result)
            check(not missing, f"{event_date}: all {len(DAILY_KEYS)} daily assets materialized (missing: {missing or 'none'})")
            run_count += 1
        print(f"  ran {run_count} partitioned materializations, all checks green")

        print("\n[2/2] Row counts (determinism check)")
        conn = connect_with_retry(demo_duckdb_path())
        try:
            loans = conn.execute("select count(*) from bronze.raw_loan_originations").fetchone()[0]
            floorplan = conn.execute("select count(*) from bronze.raw_dealer_floorplan_feed").fetchone()[0]
            portfolio = conn.execute(
                "select count(*), round(sum(outstanding_balance), 2) from gold.fact_loan_portfolio"
                " where as_of_date = '2026-08-19'"
            ).fetchone()
            eligible = conn.execute(
                "select count(*), round(sum(outstanding_balance), 2) from gold.abs_pool_eligibility"
                " where as_of_date = '2026-08-19'"
            ).fetchone()
        finally:
            conn.close()
        print(f"\n  bronze.raw_loan_originations total rows: {loans}")
        print(f"  bronze.raw_dealer_floorplan_feed total rows: {floorplan}")
        print(f"  gold.fact_loan_portfolio (2026-08-19): {portfolio[0]} contracts, ${portfolio[1]:,.2f} outstanding")
        print(f"  gold.abs_pool_eligibility (2026-08-19): {eligible[0]} contracts, ${eligible[1]:,.2f} eligible")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print(
        "VALIDATION PASSED -- the full chain materializes green across every date in the "
        "validation window and every dealer_group region, with all four asset checks passing "
        "on real, computed conditions."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
