"""End-to-end validation of the Noleggiare (FTH Group) demo ("One BI Team,
Two Companies").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- eleven of
this project's fourteen assets carry partitions, three of them on a
`date x company` `MultiPartitionsDefinition`, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves, in dependency order:

1. The six raw ingestion assets (three per company) materialize across two
   validation dates on their plain `DailyPartitionsDefinition`.
2. `dim_vehicle` and `dim_customer` -- unpartitioned, with plain `deps=` on
   partitioned upstream raw assets -- materialize standalone with no
   partition_key.
3. `fact_rental_contract` and `fact_vehicle_sale` -- daily-partitioned, with
   plain `deps=` on the unpartitioned dims -- materialize across both dates.
4. `fact_finance_consolidated_daily` and its Snowflake-variant twin --
   `date x company` multi-partitioned, with plain `deps=` on the
   single-dimension daily facts above (a deliberately mismatched
   partitions_def dependency, verified empirically to load and execute
   cleanly with no explicit PartitionMapping) -- materialize across two
   dates x two companies, including the blocking completeness check on
   `fact_rental_contract`, the blocking cross-company check on
   `dim_vehicle`, and the warning volume-band check on the consolidated
   fact.
5. `fleet_residual_value_forecast` and `qlik_cloud_export` -- both
   unpartitioned, downstream of partitioned (and multi-partitioned) facts --
   materialize standalone.

Per the brief and house rules this is a graph-first, always-green demo:
every asset body is a no-op (no synthetic data, no planted anomaly), so
there's no row count to print. What "determinism across runs" means here
instead is structural: the same asset count, check count, and pass/fail
outcome every time -- asserted below rather than assumed.

Run with: `python validate_e2e.py`
"""

import sys

import dagster as dg

from noleggiare.definitions import defs as defs_lazy

RAW_INGESTION_KEYS = [
    dg.AssetKey("rental_bookings_raw"),
    dg.AssetKey("fleet_vehicles_raw"),
    dg.AssetKey("rental_contracts_raw"),
    dg.AssetKey("vehicle_inventory_raw"),
    dg.AssetKey("dealer_sales_raw"),
    dg.AssetKey("service_orders_raw"),
]

DIM_KEYS = [
    dg.AssetKey("dim_vehicle"),
    dg.AssetKey("dim_customer"),
]

FACT_KEYS = [
    dg.AssetKey("fact_rental_contract"),
    dg.AssetKey("fact_vehicle_sale"),
]

CONSOLIDATED_KEYS = [
    dg.AssetKey("fact_finance_consolidated_daily"),
    dg.AssetKey("fact_finance_consolidated_daily_snowflake"),
]

DOWNSTREAM_UNPARTITIONED_KEYS = [
    dg.AssetKey("fleet_residual_value_forecast"),
    dg.AssetKey("qlik_cloud_export"),
]

ALL_ASSET_COUNT = (
    len(RAW_INGESTION_KEYS)
    + len(DIM_KEYS)
    + len(FACT_KEYS)
    + len(CONSOLIDATED_KEYS)
    + len(DOWNSTREAM_UNPARTITIONED_KEYS)
)  # 14

VALIDATION_DATES = ["2026-08-31", "2026-09-01"]
COMPANIES = ["noleggiare", "tomasi_auto"]

CHECK_NAMES = (
    "fact_rental_contract_completeness",
    "dim_vehicle_cross_company_consistency",
    "fact_finance_consolidated_daily_volume_band",
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
    partition_key,
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

    print("\n==> Raw ingestion (six assets, two dates)")
    for date in VALIDATION_DATES:
        run(definitions, instance, RAW_INGESTION_KEYS, date, f"raw ingestion {date}", seen_checks)

    print("\n==> Cross-company dims (unpartitioned, deps on partitioned raw)")
    run(definitions, instance, DIM_KEYS, None, "dim_vehicle + dim_customer", seen_checks)

    print("\n==> Per-company facts (daily partitioned, two dates)")
    for date in VALIDATION_DATES:
        run(definitions, instance, FACT_KEYS, date, f"per-company facts {date}", seen_checks)

    print("\n==> Cross-company consolidated fact + Snowflake twin (date x company, four keys)")
    for date in VALIDATION_DATES:
        for company in COMPANIES:
            multi_key = dg.MultiPartitionKey({"date": date, "company": company})
            run(
                definitions,
                instance,
                CONSOLIDATED_KEYS,
                multi_key,
                f"consolidated facts {date}/{company}",
                seen_checks,
            )

    print("\n==> Downstream unpartitioned (ML forecast, Qlik export)")
    run(
        definitions,
        instance,
        DOWNSTREAM_UNPARTITIONED_KEYS,
        None,
        "fleet_residual_value_forecast + qlik_cloud_export",
        seen_checks,
    )

    print("\n==> All expected checks ran")
    for name in CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print()
    if _failures:
        print(f"FAILED: {len(_failures)} assertion(s) did not hold:")
        for f in _failures:
            print(f"  - {f}")
        return 1

    print("PASSED: all assets materialized, all checks evaluated and green.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
