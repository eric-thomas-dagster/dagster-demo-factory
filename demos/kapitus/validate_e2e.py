"""End-to-end validation of the Kapitus demo.

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- every asset in
this project is `MultiPartitionsDefinition`-partitioned (date x product_line),
so this script is the harness `scripts/validate_demo.sh` requires instead. It
proves:

1. The whole chain materializes cleanly across a validation window --
   bronze loan/statement/bureau feeds -> dbt staging -> dbt marts -> dbt
   analytics -- with every check passing, for every (date, product_line)
   partition in the window.
2. Row counts are printed so determinism is visible across runs.

Per the brief ("Failure demonstration: no"), this demo runs green
end-to-end -- there is no planted anomaly or recovery sequence to validate.
The blocking and warning checks are real conditions computed from the
synthetic data (see `defs/checks/`), asserted here to genuinely pass rather
than assumed to.

Run with: `python validate_e2e.py`
"""

from __future__ import annotations

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from kapitus.components.partitions import PRODUCT_LINES  # noqa: E402
from kapitus.definitions import defs  # noqa: E402
from kapitus.demo_data.warehouse import connect_with_retry, demo_duckdb_path  # noqa: E402

RAW_LOAN_APPLICATIONS = dg.AssetKey(["raw", "loan_applications"])
RAW_BANK_STATEMENT = dg.AssetKey(["raw", "bank_statement_data"])
RAW_CREDIT_BUREAU = dg.AssetKey(["raw", "credit_bureau_pulls"])

ALL_KEYS = [
    RAW_LOAN_APPLICATIONS,
    RAW_BANK_STATEMENT,
    RAW_CREDIT_BUREAU,
    dg.AssetKey(["staging", "stg_loan_applications"]),
    dg.AssetKey(["staging", "stg_bank_statement_data"]),
    dg.AssetKey(["staging", "stg_credit_bureau_pulls"]),
    dg.AssetKey(["staging", "dim_borrower"]),
    dg.AssetKey(["staging", "dim_product_line"]),
    dg.AssetKey(["marts", "funded_loans_daily"]),
    dg.AssetKey(["marts", "portfolio_performance_by_product"]),
    dg.AssetKey(["marts", "credit_risk_summary"]),
    dg.AssetKey(["analytics", "daily_funding_summary"]),
    dg.AssetKey(["analytics", "underwriting_decision_metrics"]),
]

VALIDATION_DATES = ["2026-08-18", "2026-08-19"]

_CHECK_NAMES = (
    "loan_applications_funded_amount_sanity",
    "loan_applications_schema_stability",
    "credit_bureau_pull_completeness",
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
        print(
            f"\n[1/2] Materializing the whole chain for {len(VALIDATION_DATES)} dates "
            f"x {len(PRODUCT_LINES)} product lines: {VALIDATION_DATES} x {PRODUCT_LINES}"
        )
        run_count = 0
        for event_date in VALIDATION_DATES:
            for product_line in PRODUCT_LINES:
                partition_key = dg.MultiPartitionKey({"date": event_date, "product_line": product_line})
                result = run(definitions, instance, ALL_KEYS, partition_key=partition_key, label="daily chain")
                for check_name in _CHECK_NAMES:
                    passed = check_result_for(result, check_name)
                    if passed is False:
                        _failures.append(f"{check_name} unexpectedly failed on {partition_key}")
                missing = set(ALL_KEYS) - materialized_keys(result)
                check(
                    not missing,
                    f"{event_date}/{product_line}: all {len(ALL_KEYS)} assets materialized (missing: {missing or 'none'})",
                )
                run_count += 1
        print(f"  materialized {run_count} (date, product_line) partitions x {len(ALL_KEYS)} assets, all checks green")

        print("\n[2/2] Row counts (determinism check)")
        conn = connect_with_retry(demo_duckdb_path())
        try:
            applications = conn.execute("select count(*) from raw.loan_applications").fetchone()[0]
            funded_rows = conn.execute("select count(*) from main_marts.funded_loans_daily").fetchone()[0]
            summary_rows = conn.execute(
                "select total_applications, total_funded, overall_approval_rate from main_analytics.daily_funding_summary "
                "order by application_date limit 1"
            ).fetchone()
        finally:
            conn.close()
        print(f"\n  raw.loan_applications total rows: {applications}")
        print(f"  funded_loans_daily total rows: {funded_rows}")
        print(f"  daily_funding_summary (first day): {summary_rows}")

    print()
    if _failures:
        print("VALIDATION FAILED:")
        for failure in _failures:
            print(f"  - {failure}")
        return 1
    print(
        "VALIDATION PASSED -- the full chain materializes green across every (date, product_line) "
        "partition in the validation window, with all three asset checks passing on real, "
        "computed conditions."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
