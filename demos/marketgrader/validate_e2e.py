"""End-to-end validation of the MarketGrader.com demo ("One Index, End to End").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- 5 of this
project's 7 assets are daily-partitioned, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves, in dependency order:

1. The two genuinely-legacy assets (`legacy_sql_agent_price_ingest`,
   `legacy_sql_agent_index_calc`) are never materialized by a Dagster run --
   only observed, via `ssis_workspace_observation_sensor` -- and that the
   sensor's own key construction agrees with `get_asset_spec`'s (the
   registry gap `DemoSsisWorkspaceComponent` works around; see its
   docstring). The freshness-lag check attached by the real
   `SsisWorkspaceComponent` is evaluated directly too, same shape as
   demos/stellantis-financial-services' legacy-observer validation.
2. `raw_price_feed` / `raw_corporate_actions` materialize across two
   validation dates, writing real (if synthetic) `pyarrow.Table` rows
   through the real, PyIceberg-SQL-catalog-backed-in-demo-mode
   `iceberg_io_manager` registry component -- and their blocking
   arrival/completeness checks evaluate against those real rows, not a
   hardcoded pass.
3. `barrons_400_constituents` materializes across both dates (graph-first,
   Axis 2 stubbed body) with its warning-severity day-over-day bounds check
   evaluating real, deterministic numbers.
4. `licensee_distribution_extract` and `daily_coverage_summary` -- both
   graph-first -- materialize downstream.

Per the brief and house rules this is an always-green demo: no planted
anomaly, no failure demonstration -- including the AE notes' own suggested
"missing price file fails a check" step, which house rules override (see
README's "Correction this build makes"). Every check computed here is real,
against real (if synthetic) data.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from marketgrader.definitions import defs as defs_lazy  # noqa: E402
from marketgrader.demo_data.lakehouse import demo_iceberg_catalog_properties  # noqa: E402
from marketgrader.partitions import DAILY_PARTITIONS  # noqa: E402

LEGACY_KEYS = {
    dg.AssetKey(["legacy_sql_agent_price_ingest"]),
    dg.AssetKey(["legacy_sql_agent_index_calc"]),
}
RAW_KEYS = [dg.AssetKey(["raw_price_feed"]), dg.AssetKey(["raw_corporate_actions"])]
DOWNSTREAM_KEYS = [
    dg.AssetKey(["barrons_400_constituents"]),
    dg.AssetKey(["licensee_distribution_extract"]),
    dg.AssetKey(["daily_coverage_summary"]),
]

ALL_ASSET_COUNT = len(LEGACY_KEYS) + len(RAW_KEYS) + len(DOWNSTREAM_KEYS)  # 7

# DailyPartitionsDefinition starts 2026-09-01; today's day itself is never a
# valid partition key, so these stay fixed historical dates rather than
# "yesterday relative to now".
VALIDATION_DATES = ["2026-09-20", "2026-09-21"]

CUSTOM_CHECK_NAMES = (
    "raw_price_feed_arrival",
    "raw_corporate_actions_arrival",
    "barrons_400_constituents_membership_bounds",
)
SSIS_CHECK_NAME = "ssis_freshness_lag"

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(
    definitions: dg.Definitions,
    instance: dg.DagsterInstance,
    keys: list[dg.AssetKey],
    partition_key: str | None,
    label: str,
    seen_checks: set[str],
):
    job = definitions.resolve_implicit_job_def_def_for_assets(keys)
    if job is None:
        raise RuntimeError(f"no job resolved for {label}")
    result = job.execute_in_process(
        instance=instance,
        partition_key=partition_key,
        asset_selection=keys,
        raise_on_error=False,
    )
    if not result.success:
        raise RuntimeError(f"materialize failed: {label} partition={partition_key}")
    check(True, f"materialized {label}" + (f" partition={partition_key}" if partition_key else ""))
    for evaluation in result.get_asset_check_evaluations():
        seen_checks.add(evaluation.check_name)
        check(evaluation.passed, f"{label}: check '{evaluation.check_name}' passed")
    return result


def main() -> int:
    definitions = defs_lazy()
    seen_checks: set[str] = set()

    with dg.instance_for_test() as instance:
        print(f"==> Structural check: {ALL_ASSET_COUNT} assets declared")
        asset_graph = definitions.get_repository_def().asset_graph
        check(
            len(asset_graph.get_all_asset_keys()) == ALL_ASSET_COUNT,
            f"asset graph has exactly {ALL_ASSET_COUNT} assets",
        )

        print("\n==> [1/4] Legacy SSIS estate: never materialized by Dagster, only observed")
        sensor = definitions.get_sensor_def("ssis_workspace_observation_sensor")
        cursor = None
        freshness_results: list[bool] = []
        for _ in range(3):  # more than one full rotation of the 2 mock packages
            context = dg.build_sensor_context(instance=instance, definitions=definitions, cursor=cursor)
            result = sensor.evaluate_tick(context)
            cursor = result.cursor
            for event in result.asset_events:
                instance.report_runless_asset_event(event)
                check(
                    isinstance(event, dg.AssetObservation) and event.asset_key in LEGACY_KEYS,
                    f"ssis_workspace_observation_sensor observed a legacy asset key: {event.asset_key}",
                )

        for key in LEGACY_KEYS:
            materializations = instance.get_event_records(
                dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_MATERIALIZATION, asset_key=key)
            )
            check(len(materializations) == 0, f"{key.to_user_string()}: zero Dagster-triggered materializations, ever")
            observations = instance.get_event_records(
                dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_OBSERVATION, asset_key=key)
            )
            check(len(observations) > 0, f"{key.to_user_string()}: at least one AssetObservation recorded")

        print("\n==> Legacy freshness-lag check (native to SsisWorkspaceComponent, evaluated directly)")
        # `legacy_sql_agent_*` are pure AssetSpecs with zero Dagster-owned
        # compute (action: noop) -- Dagster can't build a job over a
        # check-only selection on a non-executable asset ("Selected keys
        # must be a subset of existing executable asset keys"), so the
        # check def is invoked directly instead, same idea as
        # demos/stellantis-financial-services evaluating its own
        # non-executable-asset check outside a job.
        repo = definitions.get_repository_def()
        for key in LEGACY_KEYS:
            check_def = repo.asset_checks_defs_by_key[dg.AssetCheckKey(asset_key=key, name=SSIS_CHECK_NAME)]
            ctx = dg.build_asset_check_context(instance=instance)
            result = check_def(ctx)
            freshness_results.append(result.passed)
            seen_checks.add(SSIS_CHECK_NAME)
        check(
            len(freshness_results) == len(LEGACY_KEYS),
            f"{SSIS_CHECK_NAME} evaluated once per legacy package (got {len(freshness_results)}/{len(LEGACY_KEYS)})",
        )
        check(all(freshness_results), f"{SSIS_CHECK_NAME} passed for every legacy package (results: {freshness_results})")

        print(f"\n==> [2/4] Target-lakehouse raw ingestion across {VALIDATION_DATES}")
        for date in VALIDATION_DATES:
            run(definitions, instance, RAW_KEYS, date, f"raw ingestion {date}", seen_checks)

        print(f"\n==> [3/4] Downstream chain (barrons_400_constituents -> egress -> reporting) across {VALIDATION_DATES}")
        for date in VALIDATION_DATES:
            run(definitions, instance, DOWNSTREAM_KEYS, date, f"downstream chain {date}", seen_checks)

        print("\n==> [4/4] All expected checks ran")
        for name in CUSTOM_CHECK_NAMES:
            check(name in seen_checks, f"check '{name}' evaluated at least once")

        print("\n==> Axis 1: real I/O landed in the demo-mode Iceberg catalog")
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog("marketgrader_lake", **demo_iceberg_catalog_properties())
        price_rows = catalog.load_table("raw.raw_price_feed").scan().to_arrow().num_rows
        corp_action_rows = catalog.load_table("raw.raw_corporate_actions").scan().to_arrow().num_rows
        print(f"  raw.raw_price_feed total rows across both partitions: {price_rows}")
        print(f"  raw.raw_corporate_actions total rows across both partitions: {corp_action_rows}")
        check(price_rows > 0, "raw_price_feed holds real rows in the Iceberg catalog")
        check(corp_action_rows >= 0, "raw_corporate_actions table exists and is queryable in the Iceberg catalog")

        print("\n==> Sensor/asset key identity (the registry gap this build works around)")
        for key in LEGACY_KEYS:
            check(
                key in asset_graph.get_all_asset_keys(),
                f"observation sensor's asset key {key.to_user_string()} matches a real asset in the graph",
            )

    print()
    if _failures:
        print(f"FAILED: {len(_failures)} assertion(s) did not hold:")
        for f in _failures:
            print(f"  - {f}")
        return 1

    print("PASSED: all assets materialized, all checks evaluated and green, real I/O confirmed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
