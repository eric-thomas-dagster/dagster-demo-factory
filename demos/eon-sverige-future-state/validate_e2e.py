"""End-to-end validation of the E.ON Sverige future-state demo.

`dg launch --assets '*'` exits immediately on any partitioned asset,
so this script is the harness `scripts/validate_demo.sh` requires. It
proves:

1. Bronze external asset declarations (`source/raw_meter_reads`,
   `source/raw_grid_load_telemetry`, `source/raw_customer_records`)
   exist as AssetSpec-only (observation-only, not materializable).
2. The four Databricks-jobs multi_assets execute across two monthly
   partitions via the demo-mode-mocked `DatabricksJobsWorkspaceComponent`.
3. The dbt layer (2 silver + 3 gold + 1 curated) materializes across
   two monthly partitions with every dbt-native test and the blocking
   `equal_rowcount` reconciliation macro passing.
4. The curated data product `customer_switching_extract` materializes
   with EU 2026/855 audit metadata columns populated.
5. The MLflow scoring + promotion assets materialize in demo mode.
6. The `grid_operations_daily_briefing` materializes with real row
   counts + peak load per region from the warehouse.
7. The `databricks_job_observation_sensor` emits AssetObservation
   events -- proving the coexistence story ("we observe runs your
   GitLab schedule triggered externally") works end-to-end.
8. Row counts across the pipeline reconcile.

Green out of the gate. No planted failures.

Run with: `uv run python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from eon_sverige_future_state.definitions import defs as defs_lazy  # noqa: E402
from eon_sverige_future_state.demo_data.warehouse import demo_duckdb_path  # noqa: E402


BRONZE_SOURCE_KEYS = [
    dg.AssetKey(["source", "raw_meter_reads"]),
    dg.AssetKey(["source", "raw_grid_load_telemetry"]),
    dg.AssetKey(["source", "raw_customer_records"]),
]

DATABRICKS_JOB_KEYS = [
    dg.AssetKey(["bronze", "meter_reads_delta"]),
    dg.AssetKey(["bronze", "grid_load_telemetry_delta"]),
    dg.AssetKey(["bronze", "customer_records_delta"]),
    dg.AssetKey(["bronze", "bronze_dq_gate"]),
]

SILVER_KEYS = [
    dg.AssetKey(["silver", "stg_meter_reads"]),
    dg.AssetKey(["silver", "stg_grid_telemetry"]),
]

GOLD_KEYS = [
    dg.AssetKey(["gold", "fct_meter_daily"]),
    dg.AssetKey(["gold", "fct_grid_hourly"]),
    dg.AssetKey(["gold", "dim_customer_current"]),
]

CURATED_KEYS = [
    dg.AssetKey(["curated", "customer_switching_extract"]),
]

MLFLOW_KEYS = [
    dg.AssetKey("grid_load_forecast_model"),
    dg.AssetKey("meter_anomaly_predictions"),
]

BRIEFING_KEYS = [
    dg.AssetKey("grid_operations_daily_briefing"),
]

ALL_ASSET_COUNT = (
    len(BRONZE_SOURCE_KEYS)
    + len(DATABRICKS_JOB_KEYS)
    + len(SILVER_KEYS)
    + len(GOLD_KEYS)
    + len(CURATED_KEYS)
    + len(MLFLOW_KEYS)
    + len(BRIEFING_KEYS)
)  # 16

VALIDATION_PARTITIONS = ["2026-04-01", "2026-05-01"]

DBT_TEST_CHECK_NAMES = (
    "not_null_stg_meter_reads_read_id",
    "unique_stg_meter_reads_read_id",
    "not_null_stg_meter_reads_meter_id",
    "not_null_stg_meter_reads_customer_id",
    "not_null_stg_meter_reads_region_code",
    "not_null_stg_meter_reads_read_date",
    "not_null_stg_meter_reads_kwh",
    "not_null_stg_grid_telemetry_telemetry_id",
    "unique_stg_grid_telemetry_telemetry_id",
    "not_null_stg_grid_telemetry_region_code",
    "not_null_stg_grid_telemetry_telemetry_date",
    "not_null_stg_grid_telemetry_hour_of_day",
    "not_null_stg_grid_telemetry_load_mw",
    "not_null_fct_meter_daily_meter_id",
    "not_null_fct_meter_daily_customer_id",
    "not_null_fct_meter_daily_read_date",
    "not_null_fct_meter_daily_region_code",
    "not_null_fct_grid_hourly_region_code",
    "not_null_fct_grid_hourly_telemetry_date",
    "not_null_fct_grid_hourly_hour_of_day",
    "not_null_dim_customer_current_customer_id",
    "unique_dim_customer_current_customer_id",
    "not_null_dim_customer_current_meter_id",
    "not_null_dim_customer_current_region_code",
    "not_null_customer_switching_extract_customer_id",
    "unique_customer_switching_extract_customer_id",
    "not_null_customer_switching_extract_switching_status",
    "not_null_customer_switching_extract_retailer_code",
    "not_null_customer_switching_extract_compliance_regulation",
    "not_null_customer_switching_extract_customer_lineage",
    "not_null_customer_switching_extract_energy_lineage",
)
CUSTOM_CHECK_NAMES = (
    "equal_rowcount_fct_meter_daily_ref_stg_meter_reads_",
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
    return result


def main() -> int:
    definitions = defs_lazy()
    instance = dg.DagsterInstance.ephemeral()

    print(f"==> Structural check: {ALL_ASSET_COUNT} assets declared")
    asset_graph = definitions.get_repository_def().asset_graph
    check(
        len(asset_graph.get_all_asset_keys()) == ALL_ASSET_COUNT,
        f"asset graph has exactly {ALL_ASSET_COUNT} assets",
    )
    for key in BRONZE_SOURCE_KEYS:
        check(
            key in asset_graph.get_all_asset_keys(),
            f"bronze source {key.to_user_string()} present in graph",
        )
        check(
            not asset_graph.get(key).is_materializable,
            f"bronze source {key.to_user_string()} is observation-only (external AssetSpec)",
        )

    seen_checks: set[str] = set()

    for partition in VALIDATION_PARTITIONS:
        print(f"\n==> Databricks jobs (demo-mode mocked execution) [{partition}]")
        result = run(
            definitions,
            instance,
            DATABRICKS_JOB_KEYS,
            partition_key=partition,
            label=f"databricks jobs {partition}",
        )
        check(True, f"{partition}: materialized {len(DATABRICKS_JOB_KEYS)} Databricks-job assets")

        # dbt silver + gold + curated are materialized as one selection per
        # partition -- the equal_rowcount generic test on fct_meter_daily
        # references stg_meter_reads via `ref()`, and dbt's `build` command
        # tries to run that test whenever either endpoint is in the selection.
        # Splitting silver from gold makes the test run against a non-existent
        # gold schema. All layers built together produces one dbt run per
        # partition where the test runs exactly once, after both models exist.
        dbt_keys = SILVER_KEYS + GOLD_KEYS + CURATED_KEYS
        print(f"\n==> Dbt layer (silver + gold + curated) [{partition}]")
        result = run(definitions, instance, dbt_keys, partition_key=partition, label=f"dbt {partition}")
        check(True, f"{partition}: materialized {len(dbt_keys)} dbt assets across silver + gold + curated")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{partition}: check '{evaluation.check_name}' passed")

        print(f"\n==> MLflow assets (demo-mode mocked) [{partition}]")
        result = run(definitions, instance, MLFLOW_KEYS, partition_key=partition, label=f"mlflow {partition}")
        check(True, f"{partition}: materialized {len(MLFLOW_KEYS)} MLflow assets")

        print(f"\n==> Business output (grid_operations_daily_briefing) [{partition}]")
        result = run(definitions, instance, BRIEFING_KEYS, partition_key=partition, label=f"briefing {partition}")
        check(True, f"{partition}: materialized {len(BRIEFING_KEYS)} business output asset(s)")

    print("\n==> Databricks observation sensor (detects externally-triggered runs)")
    sensor_def = definitions.get_sensor_def("databricks_job_observation_sensor")
    with dg.build_sensor_context(instance=instance, definitions=definitions) as sensor_ctx:
        raw = sensor_def(sensor_ctx)
    observations: list = []
    if isinstance(raw, dg.SensorResult):
        observations = list(raw.asset_events or [])
    elif raw is not None:
        for item in raw:
            if item is None:
                continue
            if isinstance(item, dg.SensorResult):
                observations.extend(item.asset_events or [])
            else:
                observations.append(item)
    check(len(observations) > 0, "databricks_job_observation_sensor emitted at least one AssetObservation")
    databricks_asset_keys = {k for k in DATABRICKS_JOB_KEYS}
    check(
        all(o.asset_key in databricks_asset_keys for o in observations),
        "every observation targets a Databricks-job asset key",
    )

    print("\n==> All expected checks ran")
    for name in DBT_TEST_CHECK_NAMES + CUSTOM_CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print("\n==> Row counts (determinism check -- real dbt SQL over fixture data)")
    conn = __import__("duckdb").connect(demo_duckdb_path(), read_only=True)
    try:
        bronze_meter_count = conn.execute("select count(*) from bronze.meter_reads").fetchone()[0]
        bronze_telemetry_count = conn.execute("select count(*) from bronze.grid_load_telemetry").fetchone()[0]
        bronze_customer_count = conn.execute("select count(*) from bronze.customer_records").fetchone()[0]
        silver_meter_count = conn.execute("select count(*) from main_silver.stg_meter_reads").fetchone()[0]
        silver_telemetry_count = conn.execute("select count(*) from main_silver.stg_grid_telemetry").fetchone()[0]
        gold_meter_count = conn.execute("select count(*) from main_gold.fct_meter_daily").fetchone()[0]
        gold_grid_count = conn.execute("select count(*) from main_gold.fct_grid_hourly").fetchone()[0]
        gold_customer_count = conn.execute("select count(*) from main_gold.dim_customer_current").fetchone()[0]
        curated_count = conn.execute("select count(*) from main_curated.customer_switching_extract").fetchone()[0]
        switching_active = conn.execute(
            "select count(*) from main_curated.customer_switching_extract "
            "where switching_status != 'none'"
        ).fetchone()[0]
    finally:
        conn.close()
    print(f"  bronze.meter_reads rows:                {bronze_meter_count}")
    print(f"  bronze.grid_load_telemetry rows:        {bronze_telemetry_count}")
    print(f"  bronze.customer_records rows:           {bronze_customer_count}")
    print(f"  silver.stg_meter_reads rows:            {silver_meter_count}")
    print(f"  silver.stg_grid_telemetry rows:         {silver_telemetry_count}")
    print(f"  gold.fct_meter_daily rows:              {gold_meter_count}")
    print(f"  gold.fct_grid_hourly rows:              {gold_grid_count}")
    print(f"  gold.dim_customer_current rows:         {gold_customer_count}")
    print(f"  curated.customer_switching_extract:     {curated_count}")
    print(f"  curated.switching_active customers:     {switching_active}")
    check(
        silver_meter_count == gold_meter_count,
        "fct_meter_daily reconciles exactly against stg_meter_reads",
    )
    check(
        silver_meter_count == bronze_meter_count,
        "stg_meter_reads reconciles exactly against bronze.meter_reads",
    )
    check(
        curated_count == bronze_customer_count,
        "customer_switching_extract has one row per customer",
    )

    print()
    if _failures:
        print(f"FAILED: {len(_failures)} assertion(s) did not hold:")
        for f in _failures:
            print(f"  - {f}")
        return 1

    print("PASSED: all assets materialized, all checks evaluated and green, row counts reconcile.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
