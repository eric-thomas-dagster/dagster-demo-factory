"""End-to-end validation of the Umicore demo ("Untangling the Spiderweb").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- eight of this
project's fourteen assets are daily-partitioned, so this script is the
harness `scripts/validate_demo.sh` requires instead. It proves, in
dependency order:

1. `adf_pipeline_stonebranch_nightly_mesh_sync` -- the real, demo-mode-
   subclassed `dagster_community_components.AzureDataFactoryComponent` --
   executes standalone (the Stonebranch-triggered legacy incumbent), and
   its observation sensor emits `AssetObservation`s for runs Dagster didn't
   trigger.
2. The four business-unit raw data products materialize across two
   validation dates: two via the real, demo-mode-subclassed
   `dagster_databricks.DatabricksWorkspaceComponent` (battery_materials,
   catalysis/AC), two via the generic `WarehouseTableAssetsComponent`
   (recycling, specialty_materials) -- one of which (recycling) depends on
   the ADF pipeline via a plain `deps=` edge, crossing the legacy/Dagster-
   owned boundary. The blocking arrival check on specialty_materials and
   the warning quality check on the AC feed both evaluate here.
3. The Databricks workspace's own observation sensor emits
   `AssetObservation`s for runs Dagster didn't trigger, against both jobs.
4. The unpartitioned dbt staging layer (four models) materializes real dbt
   SQL over the accumulated raw tables, including every dbt-native test.
5. The four daily-partitioned dbt marts materialize across both dates,
   including the blocking carbon-footprint-inputs completeness check and
   every dbt-native test. Per the same convention used across
   demos/rvu-tempcover, demos/kapitus, demos/partners-fcu, Dagster's
   partition bookkeeping here is independent of dbt's own full-refresh
   execution grain.
6. `power_bi_mesh_health_dashboard_refresh` -- a real
   `dagster_powerbi.PowerBIWorkspaceComponent` subclass, modeled as its
   refreshable semantic model -- materializes standalone, depending on a
   partitioned dbt mart via a plain `deps=` edge.
7. Row counts are printed so determinism is visible across runs.

Per the brief and house rules this is an always-green demo: no planted
anomaly, no failure demonstration. Every check computed here is real,
against real (stubbed) data -- not a hardcoded pass.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from umicore.definitions import defs as defs_lazy  # noqa: E402
from umicore.demo_data.warehouse import demo_duckdb_path  # noqa: E402

LEGACY_KEYS = [
    dg.AssetKey("adf_pipeline_stonebranch_nightly_mesh_sync"),
]

PARTITIONED_RAW_KEYS = [
    dg.AssetKey(["battery_materials", "raw_cathode_production_batch"]),
    dg.AssetKey(["catalysis", "raw_ac_daily_feed"]),
    dg.AssetKey(["recycling", "raw_recycling_batch_yield"]),
    dg.AssetKey(["specialty_materials", "raw_specialty_materials_output"]),
]

UNPARTITIONED_STAGING_KEYS = [
    dg.AssetKey(["staging", "stg_battery_materials"]),
    dg.AssetKey(["staging", "stg_catalysis_ac"]),
    dg.AssetKey(["staging", "stg_recycling"]),
    dg.AssetKey(["staging", "stg_specialty_materials"]),
]

PARTITIONED_MART_KEYS = [
    dg.AssetKey(["marts", "fct_battery_carbon_footprint_inputs"]),
    dg.AssetKey(["marts", "fct_cross_bu_consolidated_ledger"]),
    dg.AssetKey(["marts", "executive_reporting_extract"]),
    dg.AssetKey(["marts", "eu_battery_regulation_carbon_footprint_report"]),
]

REPORTING_KEYS = [
    dg.AssetKey("power_bi_mesh_health_dashboard_refresh"),
]

ALL_ASSET_COUNT = (
    len(LEGACY_KEYS)
    + len(PARTITIONED_RAW_KEYS)
    + len(UNPARTITIONED_STAGING_KEYS)
    + len(PARTITIONED_MART_KEYS)
    + len(REPORTING_KEYS)
)  # 14

# The daily partition definition starts 2026-08-01; today's day itself is
# never a valid partition key (DailyPartitionsDefinition rejects it), so
# these stay fixed historical dates rather than "yesterday relative to now".
VALIDATION_DATES = ["2026-08-14", "2026-08-15"]

DBT_TEST_CHECK_NAMES = (
    "unique_stg_battery_materials_cathode_batch_id",
    "not_null_stg_battery_materials_cathode_batch_id",
    "not_null_stg_battery_materials_production_line",
    "not_null_stg_battery_materials_output_kg",
    "unique_stg_catalysis_ac_feed_record_id",
    "not_null_stg_catalysis_ac_feed_record_id",
    "not_null_stg_catalysis_ac_catalyst_unit_id",
    "unique_stg_recycling_batch_id",
    "not_null_stg_recycling_batch_id",
    "not_null_stg_recycling_recycling_line",
    "unique_stg_specialty_materials_output_id",
    "not_null_stg_specialty_materials_output_id",
    "not_null_stg_specialty_materials_product_line",
    "unique_fct_battery_carbon_footprint_inputs_production_date",
    "not_null_fct_battery_carbon_footprint_inputs_production_date",
    "not_null_fct_battery_carbon_footprint_inputs_total_output_kg",
    "not_null_fct_cross_bu_consolidated_ledger_business_unit",
    "not_null_fct_cross_bu_consolidated_ledger_activity_date",
    "unique_executive_reporting_extract_activity_date",
    "not_null_executive_reporting_extract_activity_date",
    "unique_eu_battery_regulation_carbon_footprint_report_production_date",
    "not_null_eu_battery_regulation_carbon_footprint_report_production_date",
)
CUSTOM_CHECK_NAMES = (
    "raw_specialty_materials_output_arrival",
    "raw_ac_daily_feed_quality",
    "fct_battery_carbon_footprint_inputs_completeness",
)

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
    instance = dg.DagsterInstance.ephemeral()
    seen_checks: set[str] = set()

    print(f"==> Structural check: {ALL_ASSET_COUNT} assets declared")
    asset_graph = definitions.get_repository_def().asset_graph
    check(
        len(asset_graph.get_all_asset_keys()) == ALL_ASSET_COUNT,
        f"asset graph has exactly {ALL_ASSET_COUNT} assets",
    )

    print("\n==> Legacy incumbent (Stonebranch-triggered Azure Data Factory pipeline)")
    result = run(definitions, instance, LEGACY_KEYS, partition_key=None, label="legacy ADF pipeline", seen_checks=seen_checks)
    mats = result.get_asset_materialization_events()
    check(len(mats) == 1, "legacy ADF pipeline produced exactly one materialization")

    print("\n==> ADF observation sensor (detects Stonebranch runs Dagster didn't trigger)")
    adf_sensor_def = definitions.get_sensor_def("legacy_orchestration_observation_sensor")
    with dg.build_sensor_context(instance=instance, definitions=definitions) as sensor_ctx:
        adf_raw = list(adf_sensor_def(sensor_ctx))
    # The real component's sensor is a raw generator yielding one list per
    # object kind (pipelines/triggers/linked_services/...) plus a cursor
    # string -- flatten and keep only the actual materialization events.
    adf_observations = [
        item
        for batch in adf_raw
        if isinstance(batch, list)
        for item in batch
        if hasattr(item, "asset_key")
    ]
    check(len(adf_observations) > 0, "ADF sensor emitted at least one materialization/observation event")
    check(
        all(o.asset_key == LEGACY_KEYS[0] for o in adf_observations),
        "every ADF observation targets the same key as the materializable asset",
    )

    print("\n==> Business-unit raw layer (Databricks x2, generic stub x2, two dates)")
    for date in VALIDATION_DATES:
        run(definitions, instance, PARTITIONED_RAW_KEYS, date, f"raw layer {date}", seen_checks)

    print("\n==> Databricks workspace observation sensor (detects runs Dagster didn't trigger)")
    databricks_sensor_def = definitions.get_sensor_def("databricks_workspace_observation_sensor")
    with dg.build_sensor_context(instance=instance, definitions=definitions) as sensor_ctx:
        databricks_raw = list(databricks_sensor_def(sensor_ctx))
    # Same raw-generator shape as the ADF sensor when invoked directly in a
    # test harness -- flatten and keep only the actual observation events.
    databricks_observations = [
        item
        for batch in databricks_raw
        if isinstance(batch, list)
        for item in batch
        if hasattr(item, "asset_key")
    ]
    check(len(databricks_observations) > 0, "Databricks sensor emitted at least one AssetObservation")
    observed_keys = {o.asset_key for o in databricks_observations}
    check(
        observed_keys <= {PARTITIONED_RAW_KEYS[0], PARTITIONED_RAW_KEYS[1]},
        "every Databricks observation targets one of the two Databricks-orchestrated assets",
    )

    print("\n==> Unpartitioned dbt staging layer (four business units)")
    run(definitions, instance, UNPARTITIONED_STAGING_KEYS, None, "dbt staging", seen_checks)

    print("\n==> Partitioned dbt marts (carbon-footprint inputs, cross-BU ledger, extract, report)")
    for date in VALIDATION_DATES:
        run(definitions, instance, PARTITIONED_MART_KEYS, date, f"dbt marts {date}", seen_checks)

    print("\n==> Reporting extract (unpartitioned, deps= on partitioned upstream)")
    run(definitions, instance, REPORTING_KEYS, None, "power_bi_mesh_health_dashboard_refresh", seen_checks)

    print("\n==> All expected checks ran")
    for name in DBT_TEST_CHECK_NAMES + CUSTOM_CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print("\n==> Axis 1: real I/O landed in the demo-mode DuckDB file")
    conn = __import__("duckdb").connect(demo_duckdb_path(), read_only=True)
    try:
        cathode_count = conn.execute("select count(*) from raw.cathode_production_batch").fetchone()[0]
        ac_count = conn.execute("select count(*) from raw.ac_daily_feed").fetchone()[0]
        recycling_count = conn.execute("select count(*) from raw.recycling_batch_yield").fetchone()[0]
        specialty_count = conn.execute("select count(*) from raw.specialty_materials_output").fetchone()[0]
        ledger_count = conn.execute("select count(*) from main_marts.fct_cross_bu_consolidated_ledger").fetchone()[0]
        extract_count = conn.execute("select count(*) from main_marts.executive_reporting_extract").fetchone()[0]
    finally:
        conn.close()
    print(f"  raw.cathode_production_batch total rows: {cathode_count}")
    print(f"  raw.ac_daily_feed total rows: {ac_count}")
    print(f"  raw.recycling_batch_yield total rows: {recycling_count}")
    print(f"  raw.specialty_materials_output total rows: {specialty_count}")
    print(f"  fct_cross_bu_consolidated_ledger row count: {ledger_count}")
    print(f"  executive_reporting_extract row count: {extract_count}")
    check(
        all(c > 0 for c in (cathode_count, ac_count, recycling_count, specialty_count)),
        "all four raw tables hold real rows",
    )
    check(ledger_count == 4 * len(VALIDATION_DATES), "cross-BU ledger has one row per business unit per date")
    check(extract_count == len(VALIDATION_DATES), "executive extract has exactly one row per date")

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
