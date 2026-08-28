"""End-to-end validation of the City of Detroit DWSD demo ("One Warehouse,
Every Pipe").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- five of this
project's eleven assets are daily-partitioned, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves:

1. The five daily-partitioned assets (the meter-reading and water-quality-lab
   chains) materialize cleanly across two validation dates, including their
   two blocking completeness checks.
2. The six unpartitioned assets (the billing and work-order chains) execute
   standalone -- `fact_billing_usage` depends on the partitioned
   `fact_meter_reads` via a plain (non-argument) `deps=` edge, so this also
   proves that dependency resolves without requiring the upstream partition
   to already be materialized in this run.
3. All three asset checks run and pass.

Per the brief and house rules this is a graph-first, always-green demo: every
asset body is a no-op (no synthetic data, no planted anomaly), so there's no
row count to print. What "determinism across runs" means here instead is
structural: the same asset count, check count, and pass/fail outcome every
time -- asserted below rather than assumed.

Run with: `python validate_e2e.py`
"""

import sys

import dagster as dg

from detroit_dwsd.definitions import defs as defs_lazy

PARTITIONED_KEYS = [
    dg.AssetKey("meter_reading_extract"),
    dg.AssetKey("fact_meter_reads"),
    dg.AssetKey("water_quality_lab_extract"),
    dg.AssetKey("water_quality_compliance_daily"),
    dg.AssetKey("compliance_reporting_extract"),
]

UNPARTITIONED_KEYS = [
    dg.AssetKey("billing_system_extract"),
    dg.AssetKey("work_order_extract"),
    dg.AssetKey("dim_customer_account"),
    dg.AssetKey("work_order_status"),
    dg.AssetKey("fact_billing_usage"),
    dg.AssetKey("billing_accuracy_report"),
]

ALL_ASSET_COUNT = len(PARTITIONED_KEYS) + len(UNPARTITIONED_KEYS)  # 11

VALIDATION_DATES = ["2026-08-26", "2026-08-27"]

CHECK_NAMES = (
    "meter_reading_extract_completeness",
    "water_quality_compliance_daily_completeness",
    "billing_accuracy_report_reconciliation",
)

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(definitions: dg.Definitions, instance: dg.DagsterInstance, keys: list[dg.AssetKey], partition_key: str | None, label: str):
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


def check_passed(result: dg.ExecuteInProcessResult, check_name: str) -> bool | None:
    for evaluation in result.get_asset_check_evaluations():
        if evaluation.check_name == check_name:
            return evaluation.passed
    return None


def main() -> int:
    definitions = defs_lazy()
    instance = dg.DagsterInstance.ephemeral()

    print(f"==> Structural check: {ALL_ASSET_COUNT} assets declared")
    asset_graph = definitions.get_repository_def().asset_graph
    check(
        len(asset_graph.get_all_asset_keys()) == ALL_ASSET_COUNT,
        f"asset graph has exactly {ALL_ASSET_COUNT} assets",
    )

    seen_checks: set[str] = set()

    print("\n==> Partitioned chain (meter reads + water quality)")
    for date in VALIDATION_DATES:
        result = run(definitions, instance, PARTITIONED_KEYS, partition_key=date, label=f"partitioned chain {date}")
        check(True, f"{date}: materialized {len(PARTITIONED_KEYS)} partitioned assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{date}: check '{evaluation.check_name}' passed")

    print("\n==> Unpartitioned chain (billing + work order)")
    result = run(definitions, instance, UNPARTITIONED_KEYS, partition_key=None, label="unpartitioned chain")
    check(True, f"materialized {len(UNPARTITIONED_KEYS)} unpartitioned assets")
    for evaluation in result.get_asset_check_evaluations():
        seen_checks.add(evaluation.check_name)
        check(evaluation.passed, f"check '{evaluation.check_name}' passed")

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
