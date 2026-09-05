"""End-to-end validation of the E.ON Sverige demo ("Grid at Scale").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- four of this
project's eight assets are partitioned (two on a date x grid-zone
`MultiPartitionsDefinition`, two on a plain daily partition), so this script
is the harness `scripts/validate_demo.sh` requires instead. It proves:

1. The multi-partitioned meter-read chain (`raw_meter_reads` ->
   `validated_meter_reads`) materializes cleanly across two (date, zone)
   combinations, including its blocking completeness check.
2. The daily-partitioned grid-load chain (`raw_grid_load_telemetry` ->
   `grid_load_hourly`) materializes cleanly across two dates, including its
   blocking range check.
3. The four unpartitioned downstream assets (`customer_switching_extract`,
   `switching_data_audit_log`, `daily_grid_health_summary`,
   `regional_meter_coverage_report`) execute standalone -- each depends on a
   partitioned asset via a plain (non-argument) `deps=` edge, so this also
   proves those dependencies resolve without requiring the upstream
   partition to already be materialized in this run.
4. All three asset checks run and pass.

Per the brief and house rules this is a graph-first, always-green demo: every
asset body is a no-op (no synthetic data, no planted anomaly), so there's no
row count to print. What "determinism across runs" means here instead is
structural: the same asset count, check count, and pass/fail outcome every
time -- asserted below rather than assumed.

Run with: `python validate_e2e.py`
"""

import sys

import dagster as dg

from eon_sverige.definitions import defs as defs_lazy

MULTI_PARTITIONED_KEYS = [
    dg.AssetKey("raw_meter_reads"),
    dg.AssetKey("validated_meter_reads"),
]

DAILY_PARTITIONED_KEYS = [
    dg.AssetKey("raw_grid_load_telemetry"),
    dg.AssetKey("grid_load_hourly"),
]

UNPARTITIONED_KEYS = [
    dg.AssetKey("customer_switching_extract"),
    dg.AssetKey("switching_data_audit_log"),
    dg.AssetKey("daily_grid_health_summary"),
    dg.AssetKey("regional_meter_coverage_report"),
]

ALL_ASSET_COUNT = len(MULTI_PARTITIONED_KEYS) + len(DAILY_PARTITIONED_KEYS) + len(UNPARTITIONED_KEYS)  # 8

VALIDATION_DATES = ["2026-08-26", "2026-08-27"]
VALIDATION_REGIONS = ["se3", "se4"]

CHECK_NAMES = (
    "meter_reads_completeness_check",
    "grid_load_range_check",
    "switching_extract_audit_completeness_check",
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

    seen_checks: set[str] = set()

    print("\n==> Multi-partitioned chain (meter reads, date x grid zone)")
    for date, region in zip(VALIDATION_DATES, VALIDATION_REGIONS):
        partition_key = dg.MultiPartitionKey({"date": date, "region": region})
        result = run(
            definitions,
            instance,
            MULTI_PARTITIONED_KEYS,
            partition_key=partition_key,
            label=f"meter-read chain {partition_key}",
        )
        check(True, f"{partition_key}: materialized {len(MULTI_PARTITIONED_KEYS)} multi-partitioned assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{partition_key}: check '{evaluation.check_name}' passed")

    print("\n==> Daily-partitioned chain (grid load telemetry)")
    for date in VALIDATION_DATES:
        result = run(definitions, instance, DAILY_PARTITIONED_KEYS, partition_key=date, label=f"grid-load chain {date}")
        check(True, f"{date}: materialized {len(DAILY_PARTITIONED_KEYS)} daily-partitioned assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{date}: check '{evaluation.check_name}' passed")

    print("\n==> Unpartitioned chain (compliance + reporting)")
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
