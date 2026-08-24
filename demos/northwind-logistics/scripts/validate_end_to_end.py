"""End-to-end validation of the Northwind demo, including the recovery loop.

`dg launch --assets '*'` cannot cover a project with mixed partition schemes
in one shot -- it exits immediately with "Asset has partitions, but no
'--partition' option was provided" (verified against dagster-dg-cli 1.13.19).
This script is the partitioned equivalent, run in-process so it doesn't pay
CLI startup cost per partition.

It proves the four things the brief's validation gate asks for:

1. Everything materializes across the whole demo window.
2. The blocking `carrier_rate_arrival` check FAILS for the planted anomaly
   partition (regional_ltl_b / 2026-08-21) on the first pass.
3. Rematerializing that single partition picks up the now-arrived data.
4. Downstream recomputes cleanly and the check PASSES.

Run with: `python scripts/validate_end_to_end.py`
"""

from __future__ import annotations

import sys
import warnings

import dagster as dg
import pandas as pd

warnings.filterwarnings("ignore")

from northwind_logistics.definitions import defs  # noqa: E402
from northwind_logistics.demo_data.state import (  # noqa: E402
    ANOMALY_CARRIER,
    ANOMALY_DATE,
    DEMO_WINDOW_END,
    DEMO_WINDOW_START,
    EXPECTED_CARRIERS,
    reset_source_state,
)
from northwind_logistics.demo_data.warehouse import (  # noqa: E402
    connect_with_retry,
    demo_duckdb_path,
)

RAW_CARRIER = dg.AssetKey(["raw", "carrier_rate_raw"])
RAW_SHIPMENTS = dg.AssetKey(["raw", "shipment_events_raw"])
FIVETRAN_KEYS = [
    dg.AssetKey(["raw", "salesforce_accounts"]),
    dg.AssetKey(["raw", "zendesk_tickets"]),
    dg.AssetKey(["raw", "netsuite_gl_entries"]),
]
SNAPSHOT_KEYS = [
    dg.AssetKey(["staging", "stg_salesforce_accounts"]),
    dg.AssetKey(["staging", "stg_zendesk_tickets"]),
    dg.AssetKey(["staging", "stg_netsuite_gl_entries"]),
]
DAILY_KEYS = [
    dg.AssetKey(["staging", "carrier_rate_validated"]),
    dg.AssetKey(["staging", "shipment_events_clean"]),
    dg.AssetKey(["marts", "shipments_by_lane"]),
    dg.AssetKey(["marts", "invoice_line_items"]),
    dg.AssetKey(["marts", "carrier_cost_allocation"]),
    dg.AssetKey(["marts", "invoice_billing_nightly"]),
    dg.AssetKey(["marts", "margin_by_lane_customer"]),
]

DATES = list(pd.date_range(DEMO_WINDOW_START, DEMO_WINDOW_END, freq="D").strftime("%Y-%m-%d"))

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(definitions, instance, keys, partition_key=None, label="", expect_blocked=False):
    """Materialize `keys` for one partition.

    `expect_blocked` is for the anomaly partition: a failing BLOCKING check is
    supposed to abort the run there. That is the demo working, not the harness
    breaking, so the run's non-success is the expected outcome.
    """
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
        print("\n[1/5] Ingesting the raw layer across the whole demo window")
        for event_date in DATES:
            for carrier in EXPECTED_CARRIERS:
                run(
                    definitions,
                    instance,
                    [RAW_CARRIER],
                    partition_key=dg.MultiPartitionKey({"date": event_date, "carrier": carrier}),
                    label="carrier_rate_raw",
                )
            run(definitions, instance, [RAW_SHIPMENTS], partition_key=event_date, label="shipment_events_raw")
        run(definitions, instance, FIVETRAN_KEYS, label="fivetran")
        print(f"  materialized {len(DATES)} days x {len(EXPECTED_CARRIERS)} carriers + shipments + 3 SaaS tables")

        print("\n[2/5] Building the unpartitioned dbt snapshot layer")
        run(definitions, instance, SNAPSHOT_KEYS, label="dbt snapshot staging")

        print("\n[3/5] Building the daily dbt layers, and watching the anomaly partition")
        anomaly_check_first_pass = None
        anomaly_run_blocked = None
        for event_date in DATES:
            is_anomaly = event_date == ANOMALY_DATE
            result = run(
                definitions,
                instance,
                DAILY_KEYS,
                partition_key=event_date,
                label="dbt daily",
                expect_blocked=is_anomaly,
            )
            passed = check_result_for(result, "carrier_rate_arrival")
            if is_anomaly:
                anomaly_check_first_pass = passed
                anomaly_run_blocked = not result.success
            elif passed is False:
                _failures.append(f"carrier_rate_arrival unexpectedly failed on a clean day: {event_date}")

        print("\n[4/5] Money shot 1 -- the planted anomaly is caught, loudly")
        check(
            anomaly_check_first_pass is False,
            f"blocking carrier_rate_arrival FAILS for {ANOMALY_DATE} "
            f"(got passed={anomaly_check_first_pass})",
        )
        check(
            anomaly_run_blocked is True,
            f"the blocking check actually HALTED downstream compute for {ANOMALY_DATE} "
            "-- margin is refused, not computed wrong",
        )
        conn = connect_with_retry(demo_duckdb_path())
        try:
            blocked_rows = conn.execute(
                "select count(*) from main_marts.margin_by_lane_customer "
                "where event_date = ? and carrier = ?",
                [ANOMALY_DATE, ANOMALY_CARRIER],
            ).fetchone()[0]
        finally:
            conn.close()
        check(
            blocked_rows == 0,
            f"margin_by_lane_customer has NO rows for {ANOMALY_CARRIER} on {ANOMALY_DATE} "
            f"-- blocked, not silently wrong (got {blocked_rows})",
        )

        print("\n[5/5] Money shot 2 -- rematerialize that one partition, downstream recovers")
        run(
            definitions,
            instance,
            [RAW_CARRIER],
            partition_key=dg.MultiPartitionKey({"date": ANOMALY_DATE, "carrier": ANOMALY_CARRIER}),
            label="carrier_rate_raw recovery",
        )
        recovery = run(definitions, instance, DAILY_KEYS, partition_key=ANOMALY_DATE, label="dbt daily recovery")
        check(
            check_result_for(recovery, "carrier_rate_arrival") is True,
            f"carrier_rate_arrival now PASSES for {ANOMALY_DATE} after a plain rematerialize",
        )
        conn = connect_with_retry(demo_duckdb_path())
        try:
            recovered_rows = conn.execute(
                "select count(*) from main_marts.margin_by_lane_customer "
                "where event_date = ? and carrier = ?",
                [ANOMALY_DATE, ANOMALY_CARRIER],
            ).fetchone()[0]
            total_margin_rows = conn.execute(
                "select count(*) from main_marts.margin_by_lane_customer"
            ).fetchone()[0]
        finally:
            conn.close()
        check(
            recovered_rows > 0,
            f"margin_by_lane_customer now HAS rows for {ANOMALY_CARRIER} on {ANOMALY_DATE} "
            f"({recovered_rows} rows)",
        )
        print(f"\n  margin_by_lane_customer total rows across the window: {total_margin_rows}")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print("VALIDATION PASSED -- full materialize, blocking check catches the anomaly, "
          "and a plain rematerialize recovers it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
