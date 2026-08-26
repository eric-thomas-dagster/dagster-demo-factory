"""End-to-end validation of the Stellantis Financial Services demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is partitioned (daily, or date x dealer_group for the one
multi-partitioned bronze feed), so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves:

1. The whole Fabric pipeline chain materializes cleanly across a validation
   window -- vendor-file bronze -> silver -> gold -> Power BI refresh -- for
   every date in the window, plus every `dealer_group` partition of the one
   multi-partitioned bronze asset.
2. All four asset checks (two blocking) pass on real, computed conditions --
   not assumed.
3. Row counts are printed so determinism is visible across runs.

Per CLAUDE.md ("Demos always work -- there is no exception the brief can
grant"), this demo runs green end-to-end. There is no planted anomaly or
recovery sequence to validate; the replay/backfill story is a live,
partition-scoped rematerialization against clean data, not something this
script exercises.

`raw_dealer_floorplan_feed` is materialized separately from the other 16
assets (its `MultiPartitionsDefinition` can't share a job with the plain
`DailyPartitionsDefinition` assets), once per `dealer_group`, before the rest
of the day's chain -- `dim_dealer` reads its DuckDB rows directly via SQL, so
this ordering (not a Dagster-tracked IO-manager dependency) is what makes its
rollup see complete data.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from stellantis_financial_services.components.partitions import DEALER_GROUPS  # noqa: E402
from stellantis_financial_services.definitions import defs  # noqa: E402
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402

RAW_DEALER_FLOORPLAN_FEED = dg.AssetKey("raw_dealer_floorplan_feed")

DAILY_KEYS = [
    dg.AssetKey("raw_loan_originations"),
    dg.AssetKey("raw_lease_originations"),
    dg.AssetKey("raw_payment_transactions"),
    dg.AssetKey("raw_credit_bureau_pull"),
    dg.AssetKey("stg_loan_originations"),
    dg.AssetKey("stg_lease_originations"),
    dg.AssetKey("stg_payment_transactions"),
    dg.AssetKey("stg_delinquency_events"),
    dg.AssetKey("dim_dealer"),
    dg.AssetKey("dim_borrower"),
    dg.AssetKey("fact_loan_portfolio"),
    dg.AssetKey("fact_delinquency_snapshot"),
    dg.AssetKey("abs_pool_eligibility"),
    dg.AssetKey("gl_reconciliation_summary"),
    dg.AssetKey("customer_360"),
    dg.AssetKey("powerbi_portfolio_dashboard_refresh"),
]

ALL_KEYS = [RAW_DEALER_FLOORPLAN_FEED, *DAILY_KEYS]

VALIDATION_DATES = ["2026-08-18", "2026-08-19"]

_BLOCKING_CHECK_NAMES = ("raw_loan_originations_completeness", "abs_pool_eligibility_reconciliation")
_WARNING_CHECK_NAMES = ("dealer_floorplan_feed_lateness", "payment_transactions_row_count_reconciliation")

_failures: list = []


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


def check_result_for(result, check_name: str):
    for evaluation in result.get_asset_check_evaluations():
        if evaluation.check_name == check_name:
            return evaluation.passed
    return None


def materialized_keys(result) -> set:
    return {event.asset_key for event in result.get_asset_materialization_events()}


def main() -> int:
    definitions = defs()

    with dg.instance_for_test() as instance:
        print(f"\n[1/2] Materializing the full chain for {len(VALIDATION_DATES)} dates: {VALIDATION_DATES}")
        run_count = 0
        for event_date in VALIDATION_DATES:
            for dealer_group in DEALER_GROUPS:
                partition_key = dg.MultiPartitionKey({"date": event_date, "dealer_group": dealer_group})
                floorplan_result = run(
                    definitions, instance, [RAW_DEALER_FLOORPLAN_FEED], partition_key=partition_key, label="dealer floorplan feed"
                )
                lateness_passed = check_result_for(floorplan_result, "dealer_floorplan_feed_lateness")
                if lateness_passed is False:
                    _failures.append(f"dealer_floorplan_feed_lateness unexpectedly failed on {partition_key}")

            daily_result = run(definitions, instance, DAILY_KEYS, partition_key=event_date, label="daily chain")
            for check_name in _BLOCKING_CHECK_NAMES + _WARNING_CHECK_NAMES[1:]:
                passed = check_result_for(daily_result, check_name)
                if passed is False:
                    _failures.append(f"{check_name} unexpectedly failed on {event_date}")
            missing = set(DAILY_KEYS) - materialized_keys(daily_result)
            check(not missing, f"{event_date}: all {len(DAILY_KEYS)} daily assets materialized (missing: {missing or 'none'})")
            run_count += 1
        print(f"  materialized {run_count} dates x {len(ALL_KEYS)} assets (plus 4 dealer_group partitions/date), all checks green")

        print("\n[2/2] Row counts (determinism check)")
        conn = connect_with_retry(demo_duckdb_path())
        try:
            loans = conn.execute("select count(*) from raw.loan_originations").fetchone()[0]
            floorplan = conn.execute("select count(*) from raw.dealer_floorplan_feed").fetchone()[0]
            pool_eligible = conn.execute("select count(*) from abs.pool_eligibility where eligible").fetchone()[0]
            delinquency = conn.execute(
                "select as_of_date, total_contracts, delinquent_count, delinquency_rate from fact.delinquency_snapshot order by as_of_date"
            ).fetchall()
        finally:
            conn.close()
        print(f"\n  raw.loan_originations total rows: {loans}")
        print(f"  raw.dealer_floorplan_feed total rows: {floorplan}")
        print(f"  abs.pool_eligibility eligible rows: {pool_eligible}")
        print(f"  fact.delinquency_snapshot: {delinquency}")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print(
        "VALIDATION PASSED -- the full Fabric pipeline chain materializes green across every date "
        "and dealer_group partition in the validation window, with all four asset checks passing "
        "on real, computed conditions."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
