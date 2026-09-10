"""End-to-end validation of the Partners Federal Credit Union demo
("Dependencies, Not Duct Tape").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- seven of this
project's ten assets carry partitions, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves, in dependency order:

1. The three SQL-Server-badged raw ingestion assets materialize across two
   validation dates on their `DailyPartitionsDefinition`, writing real (if
   trivial) stub rows through the real, DuckDB-backed-in-demo-mode
   `mssql_io_manager` registry component.
2. The unpartitioned dbt staging layer (`stg_member_transactions`,
   `stg_deposit_accounts`, `stg_loan_originations`) plus `dim_member`
   materialize, running real dbt SQL over the accumulated raw tables --
   including the dbt-native `not_null`/`unique` tests and the two blocking
   completeness checks.
3. The two daily-partitioned dbt facts (`fct_daily_member_activity`,
   `fct_loan_portfolio_summary`) materialize across both dates, including
   the warning-severity reconciliation check. Per the same convention used
   by demos/rvu-tempcover, Dagster's partition bookkeeping for these two
   assets is independent of dbt's own execution grain -- the dbt SQL is a
   small full-refresh transform over the whole accumulated raw window, not
   an incremental build scoped to one partition.
4. `member_activity_reporting_extract` -- a plain, unpartitioned asset --
   materializes standalone, depending on a partitioned dbt fact via a plain
   `deps=` edge.

Per the brief and house rules this is an always-green demo: no planted
anomaly, no failure demonstration. Every check computed here is real,
against real (if stubbed) data -- not a hardcoded pass.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from partners_fcu.definitions import defs as defs_lazy  # noqa: E402
from partners_fcu.demo_data.warehouse import demo_duckdb_path  # noqa: E402

RAW_INGESTION_KEYS = [
    dg.AssetKey("raw_member_transactions"),
    dg.AssetKey("raw_deposit_accounts"),
    dg.AssetKey("raw_loan_originations"),
]

UNPARTITIONED_DBT_KEYS = [
    dg.AssetKey(["staging", "stg_member_transactions"]),
    dg.AssetKey(["staging", "stg_deposit_accounts"]),
    dg.AssetKey(["staging", "stg_loan_originations"]),
    dg.AssetKey(["marts", "dim_member"]),
]

PARTITIONED_FACT_KEYS = [
    dg.AssetKey(["marts", "fct_daily_member_activity"]),
    dg.AssetKey(["marts", "fct_loan_portfolio_summary"]),
]

REPORTING_KEYS = [
    dg.AssetKey("member_activity_reporting_extract"),
]

ALL_ASSET_COUNT = (
    len(RAW_INGESTION_KEYS) + len(UNPARTITIONED_DBT_KEYS) + len(PARTITIONED_FACT_KEYS) + len(REPORTING_KEYS)
)  # 10

# The daily partition definition starts 2026-08-01; today's day itself is
# never a valid partition key (DailyPartitionsDefinition rejects it), so
# these stay fixed historical dates rather than "yesterday relative to now".
VALIDATION_DATES = ["2026-08-14", "2026-08-15"]

DBT_TEST_CHECK_NAMES = (
    "not_null_stg_member_transactions_transaction_id",
    "unique_stg_member_transactions_transaction_id",
    "not_null_stg_member_transactions_member_id",
    "not_null_stg_member_transactions_amount",
    "not_null_stg_deposit_accounts_account_id",
    "unique_stg_deposit_accounts_account_id",
    "not_null_stg_deposit_accounts_member_id",
    "not_null_stg_loan_originations_loan_id",
    "unique_stg_loan_originations_loan_id",
    "not_null_stg_loan_originations_member_id",
    "not_null_stg_loan_originations_principal_amount",
    "not_null_dim_member_member_id",
    "unique_dim_member_member_id",
)
CUSTOM_CHECK_NAMES = (
    "raw_member_transactions_completeness",
    "raw_loan_originations_completeness",
    "fct_daily_member_activity_reconciliation",
)
PARTITIONED_DBT_TEST_CHECK_NAMES = (
    "unique_fct_daily_member_activity_transaction_date",
    "not_null_fct_daily_member_activity_transaction_date",
    "not_null_fct_loan_portfolio_summary_origination_date",
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

    print("\n==> Raw ingestion (three assets, two dates, real IO manager)")
    for date in VALIDATION_DATES:
        run(definitions, instance, RAW_INGESTION_KEYS, date, f"raw ingestion {date}", seen_checks)

    print("\n==> Unpartitioned dbt chain (staging + dim_member)")
    run(definitions, instance, UNPARTITIONED_DBT_KEYS, None, "staging + dim_member", seen_checks)

    print("\n==> Partitioned dbt facts (fct_daily_member_activity + fct_loan_portfolio_summary)")
    for date in VALIDATION_DATES:
        run(definitions, instance, PARTITIONED_FACT_KEYS, date, f"partitioned facts {date}", seen_checks)

    print("\n==> Reporting extract (unpartitioned, deps= on partitioned upstream)")
    run(definitions, instance, REPORTING_KEYS, None, "member_activity_reporting_extract", seen_checks)

    print("\n==> All expected checks ran")
    for name in DBT_TEST_CHECK_NAMES + CUSTOM_CHECK_NAMES + PARTITIONED_DBT_TEST_CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print("\n==> Axis 1: real I/O landed in the demo-mode DuckDB file")
    conn = __import__("duckdb").connect(demo_duckdb_path(), read_only=True)
    try:
        member_txn_count = conn.execute("select count(*) from raw.raw_member_transactions").fetchone()[0]
        deposit_count = conn.execute("select count(*) from raw.raw_deposit_accounts").fetchone()[0]
        loan_count = conn.execute("select count(*) from raw.raw_loan_originations").fetchone()[0]
        fct_activity_total = conn.execute(
            "select sum(transaction_count) from main_marts.fct_daily_member_activity"
        ).fetchone()[0]
        dim_member_count = conn.execute("select count(*) from main_marts.dim_member").fetchone()[0]
    finally:
        conn.close()
    print(f"  raw.raw_member_transactions total rows: {member_txn_count}")
    print(f"  raw.raw_deposit_accounts total rows: {deposit_count}")
    print(f"  raw.raw_loan_originations total rows: {loan_count}")
    print(f"  fct_daily_member_activity sum(transaction_count): {fct_activity_total}")
    print(f"  dim_member distinct member rows: {dim_member_count}")
    check(member_txn_count > 0 and deposit_count > 0 and loan_count > 0, "all three raw tables hold real rows")
    check(
        fct_activity_total == member_txn_count,
        "fct_daily_member_activity reconciles exactly against raw.raw_member_transactions",
    )
    check(dim_member_count > 0, "dim_member holds at least one distinct member")

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
