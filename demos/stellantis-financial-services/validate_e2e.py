"""End-to-end validation of the Stellantis Financial Services demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is partitioned (16 on `date`, plus `raw_dealer_floorplan_feed` on
`date` x `dealer_group`), so this script is the harness `scripts/validate_demo.sh`
requires instead. It proves:

1. The whole chain materializes cleanly across a validation window -- all
   four dealer_group partitions of the floorplan feed, then the sixteen
   date-partitioned bronze/silver/gold/reporting assets -- for every date in
   the window, with all three asset checks passing.
2. Row counts are printed so determinism is visible across runs.

Per the brief and house rules, this demo runs green end-to-end -- there is no
planted anomaly or recovery sequence to validate. The blocking and warning
checks are real conditions computed from the synthetic data (see
`defs/checks/`), asserted here to genuinely pass rather than assumed to.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from stellantis_financial_services.definitions import defs  # noqa: E402
from stellantis_financial_services.demo_data.generators import DEALER_GROUPS  # noqa: E402
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402

FLOORPLAN_KEY = dg.AssetKey(["raw_dealer_floorplan_feed"])

DAILY_KEYS = [
    dg.AssetKey(["raw_loan_originations"]),
    dg.AssetKey(["raw_lease_originations"]),
    dg.AssetKey(["raw_payment_transactions"]),
    dg.AssetKey(["raw_credit_bureau_pull"]),
    dg.AssetKey(["stg_loan_originations"]),
    dg.AssetKey(["stg_lease_originations"]),
    dg.AssetKey(["stg_payment_transactions"]),
    dg.AssetKey(["stg_delinquency_events"]),
    dg.AssetKey(["dim_dealer"]),
    dg.AssetKey(["dim_borrower"]),
    dg.AssetKey(["fact_loan_portfolio"]),
    dg.AssetKey(["fact_delinquency_snapshot"]),
    dg.AssetKey(["abs_pool_eligibility"]),
    dg.AssetKey(["gl_reconciliation_summary"]),
    dg.AssetKey(["customer_360"]),
    dg.AssetKey(["powerbi_portfolio_dashboard_refresh"]),
]

ALL_ASSET_COUNT = len(DAILY_KEYS) + 1  # + the floorplan feed

VALIDATION_DATES = ["2026-08-20", "2026-08-21"]

_CHECK_NAMES = (
    "raw_loan_originations_completeness",
    "abs_pool_eligibility_reconciliation",
    "raw_dealer_floorplan_feed_lateness",
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


def check_result_for(result, check_name: str) -> bool | None:
    for evaluation in result.get_asset_check_evaluations():
        if evaluation.check_name == check_name:
            return evaluation.passed
    return None


def materialized_keys(result) -> set:
    return {event.asset_key for event in result.get_asset_materialization_events()}


def main() -> int:
    definitions = defs()

    with dg.instance_for_test() as instance:
        print(
            f"\n[1/2] Materializing the whole chain for {len(VALIDATION_DATES)} dates: "
            f"{VALIDATION_DATES} ({ALL_ASSET_COUNT} assets/date, 4 dealer_group partitions of the floorplan feed)"
        )
        for event_date in VALIDATION_DATES:
            observed_checks: dict[str, bool] = {}

            # The floorplan feed is multi-partitioned (date x dealer_group) and
            # dim_dealer depends on all four regions for the date, so land those
            # four partitions first.
            for dealer_group in DEALER_GROUPS:
                partition_key = dg.MultiPartitionKey({"date": event_date, "dealer_group": dealer_group})
                result = run(definitions, instance, [FLOORPLAN_KEY], partition_key=partition_key, label="floorplan")
                for evaluation in result.get_asset_check_evaluations():
                    observed_checks[evaluation.check_name] = evaluation.passed
                missing = {FLOORPLAN_KEY} - materialized_keys(result)
                check(not missing, f"{event_date}/{dealer_group}: raw_dealer_floorplan_feed materialized")

            # Then the sixteen date-partitioned assets, including dim_dealer's
            # roll-up of the four floorplan partitions just landed.
            result = run(definitions, instance, DAILY_KEYS, partition_key=event_date, label="daily chain")
            for evaluation in result.get_asset_check_evaluations():
                observed_checks[evaluation.check_name] = evaluation.passed
            missing = set(DAILY_KEYS) - materialized_keys(result)
            check(not missing, f"{event_date}: all {len(DAILY_KEYS)} date-partitioned assets materialized (missing: {missing or 'none'})")

            for check_name in _CHECK_NAMES:
                passed = observed_checks.get(check_name)
                check(passed is True, f"{event_date}: check '{check_name}' ran and passed (result: {passed})")

        print(f"  materialized {len(VALIDATION_DATES)} dates x {ALL_ASSET_COUNT} assets, all checks green")

        print("\n[2/2] Row counts (determinism check)")
        conn = connect_with_retry(demo_duckdb_path())
        try:
            loans = conn.execute("select count(*) from raw.raw_loan_originations").fetchone()[0]
            floorplan = conn.execute("select count(*) from raw.raw_dealer_floorplan_feed").fetchone()[0]
            portfolio_rows = conn.execute("select count(*) from marts.fact_loan_portfolio").fetchone()[0]
            pool = conn.execute(
                "select as_of_date, dealer_group, total_balance, eligible_balance, pool_eligible "
                "from marts.abs_pool_eligibility order by as_of_date, dealer_group limit 4"
            ).fetchall()
        finally:
            conn.close()
        print(f"\n  raw.raw_loan_originations total rows: {loans}")
        print(f"  raw.raw_dealer_floorplan_feed total rows: {floorplan}")
        print(f"  marts.fact_loan_portfolio total rows: {portfolio_rows}")
        print(f"  marts.abs_pool_eligibility (first 4 rows): {pool}")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print(
        "VALIDATION PASSED -- the full chain materializes green across every date in the validation "
        "window, with all three asset checks passing on real, computed conditions."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
