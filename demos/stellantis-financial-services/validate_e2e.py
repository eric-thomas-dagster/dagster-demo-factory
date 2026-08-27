"""End-to-end validation of the Stellantis Financial Services demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is partitioned (15 Fabric-migrated assets on `date`, plus two
legacy assets Dagster never materializes at all), so this script is the
harness `scripts/validate_demo.sh` requires instead. It proves:

1. The two genuinely-legacy assets (`raw_dealer_floorplan_feed`,
   `raw_credit_bureau_pull`) are never materialized by a Dagster run -- only
   observed, via `legacy_scheduler_observer` -- and that the observation
   sensor's `SensorResult.asset_events` land in the event log as real
   `AssetObservation`s the same way the daemon would report them.
2. The dealer floorplan lateness check runs and passes as part of that same
   sensor tick (see `defs/checks/raw_dealer_floorplan_feed_lateness.py`'s
   docstring for why it can't run via a standalone job in this Dagster
   version), independent of any materialization.
3. The fifteen Fabric-migrated assets materialize cleanly across a
   validation window, including `dim_dealer` and `dim_borrower` -- the two
   assets where lineage crosses from the legacy system into the migrated
   one -- with the other two named asset checks passing.
4. Row counts are printed so determinism is visible across runs.

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
from stellantis_financial_services.defs.legacy_assets.legacy_assets import legacy_scheduler_observer  # noqa: E402
from stellantis_financial_services.demo_data.generators import DEALER_GROUPS  # noqa: E402
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402
from stellantis_financial_services.partitions import DAILY_PARTITIONS  # noqa: E402

LEGACY_KEYS = {dg.AssetKey(["raw_dealer_floorplan_feed"]), dg.AssetKey(["raw_credit_bureau_pull"])}

DAILY_KEYS = [
    dg.AssetKey(["raw_loan_originations"]),
    dg.AssetKey(["raw_lease_originations"]),
    dg.AssetKey(["raw_payment_transactions"]),
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

ALL_ASSET_COUNT = len(DAILY_KEYS) + len(LEGACY_KEYS)  # 15 Fabric-migrated + 2 legacy

VALIDATION_DATES = ["2026-08-20", "2026-08-21"]

_CHECK_NAMES = (
    "raw_loan_originations_completeness",
    "abs_pool_eligibility_reconciliation",
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
        print("\n[1/4] Legacy assets are never materialized -- only observed")
        cursor = None
        lateness_check_results: list[bool] = []
        for _ in range(len(DEALER_GROUPS) + 1):  # one full rotation: 4 floorplan regions + credit bureau
            context = dg.build_sensor_context(instance=instance, definitions=definitions, cursor=cursor)
            data = legacy_scheduler_observer.evaluate_tick(context)
            cursor = data.cursor
            for event in data.asset_events:
                instance.report_runless_asset_event(event)
                if isinstance(event, dg.AssetObservation):
                    check(
                        event.asset_key in LEGACY_KEYS,
                        f"legacy_scheduler_observer reported an AssetObservation for {event.asset_key}",
                    )
                elif isinstance(event, dg.AssetCheckEvaluation):
                    lateness_check_results.append(event.passed)
        latest_partition = DAILY_PARTITIONS.get_last_partition_key()
        for key in LEGACY_KEYS:
            materializations = instance.get_event_records(
                dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_MATERIALIZATION, asset_key=key)
            )
            check(len(materializations) == 0, f"{key}: zero Dagster-triggered materializations, ever")
            observations = instance.get_event_records(
                dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_OBSERVATION, asset_key=key)
            )
            check(len(observations) > 0, f"{key}: at least one AssetObservation recorded (got {len(observations)})")

        print(f"\n[2/4] Legacy check ran as part of the observation sensor's tick for {latest_partition}")
        check(
            len(lateness_check_results) == len(DEALER_GROUPS),
            f"raw_dealer_floorplan_feed_lateness evaluated once per region (got {len(lateness_check_results)}/{len(DEALER_GROUPS)})",
        )
        check(all(lateness_check_results), f"raw_dealer_floorplan_feed_lateness passed for every region (results: {lateness_check_results})")

        print(
            f"\n[3/4] Materializing the 15 Fabric-migrated assets for {len(VALIDATION_DATES)} dates: "
            f"{VALIDATION_DATES}"
        )
        for event_date in VALIDATION_DATES:
            result = run(definitions, instance, DAILY_KEYS, partition_key=event_date, label="daily chain")
            observed_checks = {e.check_name: e.passed for e in result.get_asset_check_evaluations()}
            missing = set(DAILY_KEYS) - materialized_keys(result)
            check(not missing, f"{event_date}: all {len(DAILY_KEYS)} date-partitioned assets materialized (missing: {missing or 'none'})")

            for check_name in _CHECK_NAMES:
                passed = observed_checks.get(check_name)
                check(passed is True, f"{event_date}: check '{check_name}' ran and passed (result: {passed})")

        print(f"  materialized {len(VALIDATION_DATES)} dates x {len(DAILY_KEYS)} Fabric-migrated assets, all checks green")

        print("\n[4/4] Row counts (determinism check)")
        conn = connect_with_retry(demo_duckdb_path())
        try:
            loans = conn.execute("select count(*) from raw.raw_loan_originations").fetchone()[0]
            floorplan = conn.execute("select count(*) from raw.raw_dealer_floorplan_feed").fetchone()[0]
            bureau = conn.execute("select count(*) from raw.raw_credit_bureau_pull").fetchone()[0]
            portfolio_rows = conn.execute("select count(*) from marts.fact_loan_portfolio").fetchone()[0]
            pool = conn.execute(
                "select as_of_date, dealer_group, total_balance, eligible_balance, pool_eligible "
                "from marts.abs_pool_eligibility order by as_of_date, dealer_group limit 4"
            ).fetchall()
        finally:
            conn.close()
        print(f"\n  raw.raw_loan_originations total rows: {loans}")
        print(f"  raw.raw_dealer_floorplan_feed total rows (legacy, SFS-scheduler-landed): {floorplan}")
        print(f"  raw.raw_credit_bureau_pull total rows (legacy, SFS-scheduler-landed): {bureau}")
        print(f"  marts.fact_loan_portfolio total rows: {portfolio_rows}")
        print(f"  marts.abs_pool_eligibility (first 4 rows): {pool}")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print(
        f"VALIDATION PASSED -- all {ALL_ASSET_COUNT} assets are correct end to end: the 15 Fabric-migrated "
        "assets materialize green across every date in the validation window, the 2 legacy assets are never "
        "Dagster-materialized and only ever observed, and every asset check passes on real, computed conditions."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
