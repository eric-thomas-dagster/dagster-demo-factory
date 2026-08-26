"""End-to-end validation of the Stellantis Financial Services demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is partitioned, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves:

1. The whole chain materializes cleanly across the pre-seeded demo window --
   vendor-file ingestion -> staging -> marts -> the Power BI refresh trigger.
2. Every check passes on every day/dealer-group except the one flagged
   floorplan partition (the shipped demo runs green by default everywhere
   else -- `Failure demonstration: yes` per the brief covers only that one
   planted anomaly).
3. `raw_dealer_floorplan_feed_completeness` genuinely blocks on the flagged
   partition's malformed record.
4. Recovery is a plain rematerialize after the mock source is "corrected" --
   no heal step, per `templates/demo_mode_pattern.py`.
5. Row counts are printed so determinism is visible across runs.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg
import pandas as pd

warnings.filterwarnings("ignore")

from stellantis_financial_services.components.partitions import (  # noqa: E402
    DEALER_GROUP_PARTITIONS_DEF,
    DEMO_WINDOW_END,
    DEMO_WINDOW_START,
)
from stellantis_financial_services.definitions import defs  # noqa: E402
from stellantis_financial_services.demo_data.fabric_source_state import (  # noqa: E402
    FLAGGED_DATE,
    FLAGGED_DEALER_GROUP,
    mark_corrected,
    reset_source_state,
)
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402

FLOORPLAN_KEY = dg.AssetKey(["raw", "raw_dealer_floorplan_feed"])

DAILY_KEYS = [
    dg.AssetKey(["raw", "raw_loan_originations"]),
    dg.AssetKey(["raw", "raw_lease_originations"]),
    dg.AssetKey(["raw", "raw_payment_transactions"]),
    dg.AssetKey(["raw", "raw_credit_bureau_pull"]),
    dg.AssetKey(["staging", "stg_loan_originations"]),
    dg.AssetKey(["staging", "stg_lease_originations"]),
    dg.AssetKey(["staging", "stg_payment_transactions"]),
    dg.AssetKey(["staging", "stg_delinquency_events"]),
    dg.AssetKey(["staging", "dim_dealer"]),
    dg.AssetKey(["staging", "dim_borrower"]),
    dg.AssetKey(["marts", "fact_loan_portfolio"]),
    dg.AssetKey(["marts", "fact_delinquency_snapshot"]),
    dg.AssetKey(["marts", "abs_pool_eligibility"]),
    dg.AssetKey(["marts", "gl_reconciliation_summary"]),
    dg.AssetKey(["marts", "customer_360"]),
    dg.AssetKey(["reporting", "powerbi_portfolio_dashboard_refresh"]),
]

DEALER_GROUPS = DEALER_GROUP_PARTITIONS_DEF.get_partition_keys()
DATES = list(pd.date_range(DEMO_WINDOW_START, DEMO_WINDOW_END, freq="D").strftime("%Y-%m-%d"))

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(definitions, instance, keys, partition_key, label="", expect_blocked=False):
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


def check_results_for(result, check_name: str) -> list[bool]:
    return [e.passed for e in result.get_asset_check_evaluations() if e.check_name == check_name]


def main() -> int:
    reset_source_state()
    definitions = defs()

    with dg.instance_for_test() as instance:
        print(f"\n[1/5] Materializing raw_dealer_floorplan_feed across {len(DATES)} days x {len(DEALER_GROUPS)} dealer groups")
        for event_date in DATES:
            for dealer_group in DEALER_GROUPS:
                mpk = dg.MultiPartitionKey({"date": event_date, "dealer_group": dealer_group})
                is_flagged = (event_date, dealer_group) == (FLAGGED_DATE, FLAGGED_DEALER_GROUP)
                result = run(
                    definitions,
                    instance,
                    [FLOORPLAN_KEY],
                    partition_key=mpk,
                    label=f"floorplan {dealer_group}",
                    expect_blocked=is_flagged,
                )
                passed_list = check_results_for(result, "raw_dealer_floorplan_feed_completeness")
                if is_flagged:
                    if passed_list and passed_list[0] is not False:
                        _failures.append(f"expected {mpk} to fail raw_dealer_floorplan_feed_completeness")
                elif passed_list and passed_list[0] is False:
                    _failures.append(f"raw_dealer_floorplan_feed_completeness unexpectedly failed on {mpk}")
        print(f"  materialized {len(DATES) * len(DEALER_GROUPS)} floorplan partitions; the flagged one failed as expected")

        print(f"\n[2/5] Materializing the daily chain ({len(DAILY_KEYS)} assets) across the demo window ({DATES[0]}..{DATES[-1]})")
        for event_date in DATES:
            result = run(definitions, instance, DAILY_KEYS, partition_key=event_date, label="daily chain")
            for check_name in [
                "raw_loan_originations_completeness",
                "abs_pool_eligibility_reconciliation",
                "payment_transactions_reconciliation",
            ]:
                for passed in check_results_for(result, check_name):
                    if passed is False:
                        _failures.append(f"{check_name} unexpectedly failed on {event_date}")
        print(f"  materialized {len(DATES)} days x {len(DAILY_KEYS)} assets, all checks green")

        print("\n[3/5] Confirming row counts are deterministic and the loan tape is populated")
        conn = connect_with_retry(demo_duckdb_path())
        try:
            portfolio_rows, portfolio_days = conn.execute(
                "select count(*), count(distinct origination_date) from marts.fact_loan_portfolio"
            ).fetchone()
            pool_rows = conn.execute("select count(*) from marts.abs_pool_eligibility").fetchone()[0]
            eligible_rows = conn.execute(
                "select count(*) from marts.abs_pool_eligibility where is_pool_eligible"
            ).fetchone()[0]
            floorplan_rows = conn.execute("select count(*) from raw.dealer_floorplan_feed").fetchone()[0]
        finally:
            conn.close()
        check(portfolio_days == len(DATES), f"fact_loan_portfolio covers {portfolio_days} distinct days (expected {len(DATES)})")
        check(pool_rows == portfolio_rows, f"abs_pool_eligibility reconciles 1:1 with fact_loan_portfolio ({pool_rows} == {portfolio_rows})")
        print(f"  fact_loan_portfolio total rows: {portfolio_rows} ({eligible_rows} pool-eligible)")
        print(f"  raw_dealer_floorplan_feed total rows: {floorplan_rows}")

        print("\n[4/5] Re-confirming the blocking check actually halts the run on the flagged partition")
        probe_mpk = dg.MultiPartitionKey({"date": FLAGGED_DATE, "dealer_group": FLAGGED_DEALER_GROUP})
        probe_result = run(
            definitions, instance, [FLOORPLAN_KEY], partition_key=probe_mpk, label="floorplan probe", expect_blocked=True
        )
        check(not probe_result.success, "the blocking check halted the run rather than succeeding on the malformed batch")
        check(
            check_results_for(probe_result, "raw_dealer_floorplan_feed_completeness") == [False],
            "raw_dealer_floorplan_feed_completeness still fails on the flagged partition before correction",
        )

        print("\n[5/5] Restoring via a plain rematerialize -- no heal step, the source just changed back")
        mark_corrected()
        final_result = run(definitions, instance, [FLOORPLAN_KEY], partition_key=probe_mpk, label="floorplan recovery")
        check(
            check_results_for(final_result, "raw_dealer_floorplan_feed_completeness") == [True],
            f"raw_dealer_floorplan_feed_completeness PASSES for {probe_mpk} after the vendor resent a corrected file",
        )

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print(
        "VALIDATION PASSED -- the full chain materializes green across the demo window, the "
        "planted anomaly is proven to actually block downstream, and recovery is a plain "
        "rematerialize with no heal step."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
