"""End-to-end validation of the Stellantis Financial Services demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is partitioned, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves:

1. The whole chain materializes cleanly across a validation window --
   bronze vendor feeds -> dbt staging -> dbt marts -> the Power BI refresh
   trigger -- with every check passing, on every date except the one
   planted anomaly.
2. `dealer_floorplan_completeness` genuinely blocks on the planted-anomaly
   partition: the check fails, and `dim_dealer` (which depends on the
   checked asset via dbt's `source()` lineage) gets no materialization
   event for that run -- Dagster actually skipped it, not just logged a
   warning.
3. Recovery is a plain rematerialize of the one partition, once the mock
   vendor source is "corrected" -- no heal step, no reset job, matching
   CLAUDE.md's idempotency rule.
4. Row counts are printed so determinism is visible across runs.

Run with: `python validate_e2e.py`
"""

from __future__ import annotations

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from stellantis_financial_services.definitions import defs  # noqa: E402
from stellantis_financial_services.demo_data.vendor_state import ANOMALY_DATE, mark_corrected  # noqa: E402
from stellantis_financial_services.demo_data.vendor_state import reset_source_state  # noqa: E402
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402

RAW_LOAN = dg.AssetKey(["raw", "loan_originations"])
RAW_LEASE = dg.AssetKey(["raw", "lease_originations"])
RAW_PAYMENTS = dg.AssetKey(["raw", "payment_transactions"])
RAW_FLOORPLAN = dg.AssetKey(["raw", "dealer_floorplan_feed"])
RAW_BUREAU = dg.AssetKey(["raw", "credit_bureau_pull"])
STG_DIM_DEALER = dg.AssetKey(["staging", "dim_dealer"])

ALL_KEYS = [
    RAW_LOAN, RAW_LEASE, RAW_PAYMENTS, RAW_FLOORPLAN, RAW_BUREAU,
    dg.AssetKey(["staging", "stg_loan_originations"]),
    dg.AssetKey(["staging", "stg_lease_originations"]),
    dg.AssetKey(["staging", "stg_payment_transactions"]),
    dg.AssetKey(["staging", "stg_delinquency_events"]),
    STG_DIM_DEALER,
    dg.AssetKey(["staging", "dim_borrower"]),
    dg.AssetKey(["marts", "fact_loan_portfolio"]),
    dg.AssetKey(["marts", "fact_delinquency_snapshot"]),
    dg.AssetKey(["marts", "abs_pool_eligibility"]),
    dg.AssetKey(["marts", "gl_reconciliation_summary"]),
    dg.AssetKey(["marts", "customer_360"]),
    dg.AssetKey(["reporting", "powerbi_portfolio_dashboard_refresh"]),
]

CLEAN_DATES = ["2026-08-18", "2026-08-19", "2026-08-21"]

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(definitions, instance, keys, partition_key=None, label="", expect_blocked=False):
    job = definitions.resolve_implicit_job_def_def_for_assets(keys)
    if job is None:
        raise RuntimeError(f"no job resolved for {keys}")
    result = job.execute_in_process(
        instance=instance,
        partition_key=partition_key,
        asset_selection=list(keys),
        raise_on_error=False,
    )
    if not result.success and not expect_blocked:
        raise RuntimeError(f"materialize failed: {label or keys} partition={partition_key}")
    return result


def check_result_for(result, check_name: str) -> bool | None:
    for evaluation in result.get_asset_check_evaluations():
        if evaluation.check_name == check_name:
            return evaluation.passed
    return None


def materialized_keys(result) -> set:
    return {event.asset_key for event in result.get_asset_materialization_events()}


def main() -> int:
    reset_source_state()
    definitions = defs()

    with dg.instance_for_test() as instance:
        print(f"\n[1/4] Materializing the whole chain on {len(CLEAN_DATES)} clean dates: {CLEAN_DATES}")
        for event_date in CLEAN_DATES:
            result = run(definitions, instance, ALL_KEYS, partition_key=event_date, label="daily chain")
            for check_name in (
                "loan_originations_completeness",
                "dealer_floorplan_completeness",
                "dealer_floorplan_lateness",
                "abs_pool_eligibility_completeness",
            ):
                passed = check_result_for(result, check_name)
                if passed is False:
                    _failures.append(f"{check_name} unexpectedly failed on {event_date}")
            missing = set(ALL_KEYS) - materialized_keys(result)
            check(not missing, f"{event_date}: all {len(ALL_KEYS)} assets materialized (missing: {missing or 'none'})")
        print(f"  materialized {len(CLEAN_DATES)} clean dates x {len(ALL_KEYS)} assets, all checks green")

        print(f"\n[2/4] Proving dealer_floorplan_completeness genuinely blocks on {ANOMALY_DATE} (planted anomaly)")
        anomaly_result = run(
            definitions,
            instance,
            ALL_KEYS,
            partition_key=ANOMALY_DATE,
            label="anomaly-date full chain",
            expect_blocked=True,
        )
        check(not anomaly_result.success, f"the run on {ANOMALY_DATE} did not succeed (blocking check should have stopped it)")
        check(
            check_result_for(anomaly_result, "dealer_floorplan_completeness") is False,
            "dealer_floorplan_completeness FAILS on the malformed batch",
        )
        materialized = materialized_keys(anomaly_result)
        check(
            STG_DIM_DEALER not in materialized,
            f"dim_dealer got NO materialization event for {ANOMALY_DATE} -- Dagster actually skipped it, not just warned",
        )
        check(RAW_FLOORPLAN in materialized, "raw_dealer_floorplan_feed itself still materialized (the check runs on real output, not a stub)")

        print(f"\n[3/4] Recovery: vendor resends the corrected file for {ANOMALY_DATE}, rematerialize just that partition")
        mark_corrected()
        recovery_bronze = run(
            definitions,
            instance,
            [RAW_FLOORPLAN],
            partition_key=ANOMALY_DATE,
            label="recovery: bronze only",
        )
        check(
            check_result_for(recovery_bronze, "dealer_floorplan_completeness") is True,
            f"dealer_floorplan_completeness PASSES for {ANOMALY_DATE} after the corrected file lands",
        )

        print(f"\n[4/4] Downstream recomputes for {ANOMALY_DATE} now that bronze is clean -- no heal step, just a rematerialize")
        recovery_full = run(
            definitions,
            instance,
            ALL_KEYS,
            partition_key=ANOMALY_DATE,
            label="recovery: full chain",
        )
        check(recovery_full.success, f"the full chain succeeds for {ANOMALY_DATE} after recovery")
        recovered_materialized = materialized_keys(recovery_full)
        check(STG_DIM_DEALER in recovered_materialized, "dim_dealer now materializes for the recovered partition")

        conn = connect_with_retry(demo_duckdb_path())
        try:
            portfolio_rows = conn.execute("select count(*) from main_marts.fact_loan_portfolio").fetchone()[0]
            eligible_rows, ineligible_rows = conn.execute(
                "select sum(case when pool_eligible then 1 else 0 end), "
                "sum(case when not pool_eligible then 1 else 0 end) from main_marts.abs_pool_eligibility"
            ).fetchone()
            dealer_rows = conn.execute("select count(*) from main_staging.dim_dealer").fetchone()[0]
        finally:
            conn.close()
        print(f"\n  fact_loan_portfolio total rows: {portfolio_rows}")
        print(f"  abs_pool_eligibility: {eligible_rows} eligible, {ineligible_rows} ineligible")
        print(f"  dim_dealer total rows: {dealer_rows}")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print(
        "VALIDATION PASSED -- the full chain materializes green across the validation window, the "
        "blocking check genuinely stops downstream computation on the planted anomaly, and recovery "
        "is a plain rematerialize of the one affected partition."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
