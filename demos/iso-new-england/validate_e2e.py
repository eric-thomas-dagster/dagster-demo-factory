"""End-to-end validation of the ISO New England demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is partitioned, so this script is the harness `scripts/validate_demo.sh`
requires instead. It proves:

1. The whole chain materializes cleanly across the pre-seeded demo window --
   Oracle extract -> Postgres landing -> dbt staging/intermediate/marts ->
   `platform_status_report`.
2. The blocking `staged_readings_completeness` check passes on every day (the
   shipped demo runs green by default -- no staged failure was requested for
   this brief).
3. `staged_readings_completeness` genuinely blocks when the batch actually is
   incomplete -- proven by forcing one partition through with a truncated
   frame, in isolation, without touching the committed demo state.
4. Row counts are printed so determinism is visible across runs.

Run with: `python validate_e2e.py`
"""

from __future__ import annotations

import sys
import warnings

import dagster as dg
import pandas as pd

warnings.filterwarnings("ignore")

from iso_new_england.definitions import defs  # noqa: E402
from iso_new_england.demo_data.feed_state import (  # noqa: E402
    DEFAULT_WINDOW_END,
    DEFAULT_WINDOW_START,
    reset_source_state,
)
from iso_new_england.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402

RAW_ORACLE = dg.AssetKey(["raw", "legacy_oracle_extract"])
RAW_FEED = dg.AssetKey(["raw", "external_feed_raw"])
STAGED_READINGS = dg.AssetKey(["staged", "staged_readings"])
STAGED_REFERENCE = dg.AssetKey(["staged", "staged_reference"])

DAILY_KEYS = [
    RAW_ORACLE,
    RAW_FEED,
    STAGED_READINGS,
    STAGED_REFERENCE,
    dg.AssetKey(["staging", "stg_readings"]),
    dg.AssetKey(["staging", "stg_reference"]),
    dg.AssetKey(["intermediate", "int_readings_validated"]),
    dg.AssetKey(["intermediate", "int_daily_rollup"]),
    dg.AssetKey(["marts", "mart_daily_operations_summary"]),
    dg.AssetKey(["marts", "mart_source_reliability"]),
    dg.AssetKey(["marts", "mart_capacity_trend"]),
    dg.AssetKey(["marts", "platform_status_report"]),
]

DATES = list(pd.date_range(DEFAULT_WINDOW_START, DEFAULT_WINDOW_END, freq="D").strftime("%Y-%m-%d"))

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(definitions, instance, keys, partition_key=None, label="", expect_blocked=False):
    job = definitions.resolve_implicit_job_def_def_for_assets(keys)
    if job is None:
        raise RuntimeError(f"no job resolved for {keys}")
    result = job.execute_in_process(
        instance=instance,
        partition_key=partition_key,
        asset_selection=list(keys),
        raise_on_error=False,
    )
    if not result.success and not expect_blocked:
        raise RuntimeError(f"materialize failed: {label or keys} partition={partition_key}")
    return result


def check_result_for(result, check_name: str) -> bool | None:
    for evaluation in result.get_asset_check_evaluations():
        if evaluation.check_name == check_name:
            return evaluation.passed
    return None


def main() -> int:
    reset_source_state()
    definitions = defs()

    with dg.instance_for_test() as instance:
        print(f"\n[1/4] Materializing the whole chain across the demo window ({DATES[0]}..{DATES[-1]})")
        for event_date in DATES:
            result = run(definitions, instance, DAILY_KEYS, partition_key=event_date, label="daily chain")
            completeness_passed = check_result_for(result, "staged_readings_completeness")
            arrival_passed = check_result_for(result, "external_feed_arrival")
            if completeness_passed is False:
                _failures.append(f"staged_readings_completeness unexpectedly failed on {event_date}")
            if arrival_passed is False:
                _failures.append(f"external_feed_arrival unexpectedly failed on {event_date}")
        print(f"  materialized {len(DATES)} days x {len(DAILY_KEYS)} assets, all checks green")

        print("\n[2/4] Confirming platform_status_report has one row per day, deterministically")
        conn = connect_with_retry(demo_duckdb_path())
        try:
            report_rows = conn.execute(
                "select count(*), count(distinct event_date) from main_marts.platform_status_report"
            ).fetchone()
            statuses = conn.execute(
                "select platform_status, count(*) from main_marts.platform_status_report group by 1 order by 1"
            ).fetchall()
            readings_rows = conn.execute("select count(*) from staged.staged_readings").fetchone()[0]
            advisory_rows = conn.execute("select count(*) from staged.staged_reference").fetchone()[0]
        finally:
            conn.close()
        check(report_rows[0] == len(DATES), f"platform_status_report has {report_rows[0]} rows (expected {len(DATES)})")
        check(report_rows[1] == len(DATES), f"platform_status_report covers {report_rows[1]} distinct days (expected {len(DATES)})")
        print(f"  staged_readings total rows: {readings_rows}")
        print(f"  staged_reference total rows: {advisory_rows}")
        print(f"  platform_status_report status breakdown: {statuses}")

        print("\n[3/4] Proving staged_readings_completeness actually blocks on a truncated batch")
        # Truncates the RAW table (what the landing step copies FROM) for one
        # partition, then rematerializes only `staged_readings` -- a real
        # Dagster execution of the real blocking check against a genuinely
        # incomplete batch, not a simulated context. This is a build-time
        # proof that the check works, not a persisted anomaly: the brief asks
        # for a green live walkthrough (no staged failure requested), and
        # step 4 restores the source and rematerializes to prove the recovery
        # is a plain rematerialize, per CLAUDE.md's idempotency rule.
        probe_date = DATES[-1]
        conn = connect_with_retry(demo_duckdb_path())
        try:
            full_count = conn.execute(
                "select count(*) from raw.legacy_oracle_extract where event_date = ?", [probe_date]
            ).fetchone()[0]
            conn.execute(
                "delete from raw.legacy_oracle_extract where event_date = ? and reporting_point_id > 'RP-002'",
                [probe_date],
            )
        finally:
            conn.close()

        probe_result = run(
            definitions,
            instance,
            [STAGED_READINGS],
            partition_key=probe_date,
            label="staged_readings probe on truncated raw batch",
            expect_blocked=True,
        )
        probe_passed = check_result_for(probe_result, "staged_readings_completeness")
        check(probe_passed is False, f"staged_readings_completeness FAILS on a truncated batch (got passed={probe_passed})")
        check(not probe_result.success, "the blocking check halted the run rather than succeeding on bad input")

        print("\n[4/4] Restoring via a plain rematerialize -- no heal step, the source just changed back")
        run(definitions, instance, [RAW_ORACLE], partition_key=probe_date, label="restore raw partition")
        final_result = run(
            definitions,
            instance,
            [STAGED_READINGS],
            partition_key=probe_date,
            label="staged_readings recovery",
        )
        check(
            check_result_for(final_result, "staged_readings_completeness") is True,
            f"staged_readings_completeness PASSES again for {probe_date} after restoring the batch",
        )
        conn = connect_with_retry(demo_duckdb_path())
        try:
            restored_count = conn.execute(
                "select count(*) from staged.staged_readings where event_date = ?", [probe_date]
            ).fetchone()[0]
        finally:
            conn.close()
        check(restored_count == full_count, f"staged_readings restored to {restored_count} rows (expected {full_count})")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print("VALIDATION PASSED -- full chain materializes green across the demo window, and the "
          "blocking check is proven to actually catch an incomplete batch.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
