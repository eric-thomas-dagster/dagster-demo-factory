"""End-to-end validation of the Neighborhood Intelligence (NYC Yellow Taxi) demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset
in this project is monthly-partitioned, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves:

1. Every layer (bronze external assets, silver dbt, gold dbt, report dbt)
   materializes cleanly for two monthly partitions (2022-01-01 and
   2022-02-01, both non-empty in the seeded fixture).
2. Every asset check evaluates -- the dbt-native uniqueness + not_null
   contracts on `fct_trips.trip_id` (blocking), every `stg_*` and
   `agg_*` and `report_*` schema.yml test, and the native Python
   reconciliation check between `stg_yellow_trips` and `fct_trips`
   (also blocking).
3. Row counts across the pipeline reconcile -- the raw fixture, the
   silver typed view, and the gold fact table all report the same trip
   count, so the enrichment left join isn't silently dropping rows.

Per the brief and house rules this is an always-green demo: no planted
anomaly, no failure demonstration. Every check computed here is real,
against real (fixture) data -- not a hardcoded pass.

Run with: `uv run python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from neighborhood_intelligence_taxi.definitions import defs as defs_lazy  # noqa: E402
from neighborhood_intelligence_taxi.demo_data.warehouse import demo_duckdb_path  # noqa: E402

BRONZE_KEYS = [
    dg.AssetKey("source_yellow_trips"),
    dg.AssetKey("source_taxi_zone_lookup"),
]

SILVER_KEYS = [
    dg.AssetKey(["silver", "stg_yellow_trips"]),
    dg.AssetKey(["silver", "stg_taxi_zones"]),
]

GOLD_KEYS = [
    dg.AssetKey(["gold", "fct_trips"]),
    dg.AssetKey(["gold", "agg_daily_zone"]),
    dg.AssetKey(["gold", "agg_hourly_demand"]),
    dg.AssetKey(["gold", "agg_vendor_performance"]),
]

REPORT_KEYS = [
    dg.AssetKey(["report", "report_daily_summary"]),
    dg.AssetKey(["report", "report_top_zones"]),
]

ALL_ASSET_COUNT = len(BRONZE_KEYS) + len(SILVER_KEYS) + len(GOLD_KEYS) + len(REPORT_KEYS)  # 10

# Two monthly partitions from the fixture window (2022-01 through 2022-03).
VALIDATION_PARTITIONS = ["2022-01-01", "2022-02-01"]

# dbt-native tests emitted from schema.yml -- every one should evaluate at
# least once across the pipeline.
DBT_TEST_CHECK_NAMES = (
    "not_null_stg_yellow_trips_trip_id",
    "unique_stg_yellow_trips_trip_id",
    "not_null_stg_yellow_trips_vendor_id",
    "not_null_stg_yellow_trips_pickup_datetime",
    "not_null_stg_yellow_trips_pickup_location_id",
    "not_null_stg_yellow_trips_dropoff_location_id",
    "not_null_stg_taxi_zones_zone_id",
    "unique_stg_taxi_zones_zone_id",
    "not_null_stg_taxi_zones_borough",
    "not_null_fct_trips_trip_id",
    "unique_fct_trips_trip_id",
    "not_null_fct_trips_pickup_datetime",
    "not_null_fct_trips_pickup_date",
    "not_null_fct_trips_pickup_location_id",
    "not_null_fct_trips_dropoff_location_id",
    "not_null_fct_trips_vendor_id",
    "not_null_agg_daily_zone_pickup_date",
    "not_null_agg_daily_zone_pickup_location_id",
    "not_null_agg_hourly_demand_pickup_borough",
    "not_null_agg_hourly_demand_pickup_hour",
    "not_null_agg_vendor_performance_vendor_id",
    "unique_agg_vendor_performance_vendor_id",
    "not_null_report_daily_summary_pickup_date",
    "unique_report_daily_summary_pickup_date",
    "not_null_report_daily_summary_trip_count",
    "not_null_report_top_zones_pickup_location_id",
    "unique_report_top_zones_pickup_location_id",
    "not_null_report_top_zones_trip_count_rank",
)
CUSTOM_CHECK_NAMES = (
    # Generic dbt test declared on fct_trips in gold/_schema.yml -- the
    # generated check name is `<test>_<model>_<args>` per dbt convention.
    "equal_rowcount_fct_trips_ref_stg_yellow_trips_",
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
    for key in BRONZE_KEYS:
        check(
            key in asset_graph.get_all_asset_keys(),
            f"bronze external asset {key.to_user_string()} present in graph",
        )
        # Confirm each bronze asset is external (AssetSpec-only, no executable body).
        # External assets aren't materializable in a job -- they represent BigQuery
        # tables Dagster observes but doesn't produce -- so this loop asserts their
        # declaration rather than executing them.
        check(
            not asset_graph.get(key).is_materializable,
            f"bronze external asset {key.to_user_string()} is observation-only (external)",
        )

    seen_checks: set[str] = set()

    for partition in VALIDATION_PARTITIONS:
        print(f"\n==> Silver dbt models [{partition}]")
        result = run(definitions, instance, SILVER_KEYS, partition_key=partition, label=f"silver {partition}")
        check(True, f"{partition}: materialized {len(SILVER_KEYS)} silver assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{partition}: check '{evaluation.check_name}' passed")

        print(f"\n==> Gold dbt models [{partition}]")
        result = run(definitions, instance, GOLD_KEYS, partition_key=partition, label=f"gold {partition}")
        check(True, f"{partition}: materialized {len(GOLD_KEYS)} gold assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{partition}: check '{evaluation.check_name}' passed")

        print(f"\n==> Report dbt models [{partition}]")
        result = run(definitions, instance, REPORT_KEYS, partition_key=partition, label=f"report {partition}")
        check(True, f"{partition}: materialized {len(REPORT_KEYS)} report assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{partition}: check '{evaluation.check_name}' passed")

    print("\n==> All expected checks ran")
    for name in DBT_TEST_CHECK_NAMES + CUSTOM_CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print("\n==> Row counts (determinism check -- real dbt SQL over fixture data)")
    conn = __import__("duckdb").connect(demo_duckdb_path(), read_only=True)
    try:
        raw_trip_count = conn.execute("select count(*) from raw.yellow_trips").fetchone()[0]
        raw_zone_count = conn.execute("select count(*) from raw.taxi_zone_lookup").fetchone()[0]
        silver_trip_count = conn.execute("select count(*) from main_silver.stg_yellow_trips").fetchone()[0]
        silver_zone_count = conn.execute("select count(*) from main_silver.stg_taxi_zones").fetchone()[0]
        gold_trip_count = conn.execute("select count(*) from main_gold.fct_trips").fetchone()[0]
        gold_daily_zone_rows = conn.execute("select count(*) from main_gold.agg_daily_zone").fetchone()[0]
        gold_hourly_rows = conn.execute("select count(*) from main_gold.agg_hourly_demand").fetchone()[0]
        gold_vendor_rows = conn.execute("select count(*) from main_gold.agg_vendor_performance").fetchone()[0]
        report_daily_rows = conn.execute("select count(*) from main_report.report_daily_summary").fetchone()[0]
        report_top_rows = conn.execute("select count(*) from main_report.report_top_zones").fetchone()[0]
    finally:
        conn.close()
    print(f"  raw.yellow_trips rows:              {raw_trip_count}")
    print(f"  raw.taxi_zone_lookup rows:          {raw_zone_count}")
    print(f"  silver.stg_yellow_trips rows:       {silver_trip_count}")
    print(f"  silver.stg_taxi_zones rows:         {silver_zone_count}")
    print(f"  gold.fct_trips rows:                {gold_trip_count}")
    print(f"  gold.agg_daily_zone rows:           {gold_daily_zone_rows}")
    print(f"  gold.agg_hourly_demand rows:        {gold_hourly_rows}")
    print(f"  gold.agg_vendor_performance rows:   {gold_vendor_rows}")
    print(f"  report.report_daily_summary rows:   {report_daily_rows}")
    print(f"  report.report_top_zones rows:       {report_top_rows}")
    check(
        silver_trip_count == gold_trip_count,
        "fct_trips reconciles exactly against stg_yellow_trips (row count match)",
    )
    check(
        silver_trip_count == raw_trip_count,
        "stg_yellow_trips reconciles exactly against raw.yellow_trips (row count match)",
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
