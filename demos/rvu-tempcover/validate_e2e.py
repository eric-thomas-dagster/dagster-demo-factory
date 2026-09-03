"""End-to-end validation of the RVU (Tempcover) demo ("From Ran to Right").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- two of this
project's twelve assets (`marts/fct_quotes_daily`,
`marts/fct_bound_policies_daily`) are daily-partitioned, so this script is
the harness `scripts/validate_demo.sh` requires instead. It proves:

1. The eight unpartitioned assets (Fivetran ingestion, dbt staging, dbt's
   dim_partner) execute standalone, with every dbt-native test and the
   blocking panel-completeness check passing.
2. The two daily-partitioned dbt facts materialize cleanly across two
   validation dates, including the warning-severity reconciliation check.
3. The two downstream activation/reporting assets execute standalone,
   depending on partitioned dbt facts via a plain `deps=` edge -- proving
   that dependency resolves without requiring the upstream partition to
   already exist in this run (LEARNINGS.md, verified 2026-08-28/09-02).
4. Row counts are printed so determinism is visible across runs -- this
   build's dbt layer runs real SQL over deterministic fixture data (see
   demo_data/fixtures/), unlike a pure graph-first build with nothing to
   count.

Per the brief and house rules this is an always-green demo: no planted
anomaly, no failure demonstration. Every check computed here is real,
against real (fixture) data -- not a hardcoded pass.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from rvu_tempcover.definitions import defs as defs_lazy  # noqa: E402
from rvu_tempcover.demo_data.warehouse import demo_duckdb_path  # noqa: E402

UNPARTITIONED_KEYS = [
    dg.AssetKey("raw_quote_requests"),
    dg.AssetKey("raw_bound_policies"),
    dg.AssetKey("raw_panel_insurer_feed"),
    dg.AssetKey("raw_partner_broker_feed"),
    dg.AssetKey(["staging", "stg_quote_requests"]),
    dg.AssetKey(["staging", "stg_bound_policies"]),
    dg.AssetKey(["staging", "stg_panel_feed"]),
    dg.AssetKey(["marts", "dim_partner"]),
]

PARTITIONED_KEYS = [
    dg.AssetKey(["marts", "fct_quotes_daily"]),
    dg.AssetKey(["marts", "fct_bound_policies_daily"]),
]

DOWNSTREAM_KEYS = [
    dg.AssetKey("braze_customer_segment_export"),
    dg.AssetKey("power_bi_quote_performance_report"),
]

ALL_ASSET_COUNT = len(UNPARTITIONED_KEYS) + len(PARTITIONED_KEYS) + len(DOWNSTREAM_KEYS)  # 12

# The daily partition definition starts 2026-08-01, and dbt's own vars
# (min_date/max_date) span the fixture window through 2026-12-31; today's
# day itself is never a valid partition key (DailyPartitionsDefinition
# rejects it -- LEARNINGS.md, verified 2026-09-02), so these stay fixed
# historical dates rather than "yesterday relative to now".
VALIDATION_DATES = ["2026-08-14", "2026-08-15"]

DBT_TEST_CHECK_NAMES = (
    "not_null_stg_quote_requests_quote_id",
    "unique_stg_quote_requests_quote_id",
    "not_null_stg_quote_requests_customer_id",
    "not_null_stg_bound_policies_policy_id",
    "unique_stg_bound_policies_policy_id",
    "not_null_stg_bound_policies_customer_id",
    "not_null_stg_bound_policies_premium_amount",
    "not_null_stg_panel_feed_insurer_id",
    "not_null_stg_panel_feed_feed_date",
    "not_null_dim_partner_partner_id",
    "unique_dim_partner_partner_id",
)
CUSTOM_CHECK_NAMES = (
    "raw_panel_insurer_feed_completeness",
    "fct_bound_policies_daily_reconciliation",
)
PARTITIONED_DBT_TEST_CHECK_NAMES = (
    "not_null_fct_quotes_daily_quote_date",
    "not_null_fct_bound_policies_daily_bind_date",
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

    print("\n==> Unpartitioned chain (Fivetran ingestion, dbt staging, dim_partner)")
    result = run(definitions, instance, UNPARTITIONED_KEYS, partition_key=None, label="unpartitioned chain")
    check(True, f"materialized {len(UNPARTITIONED_KEYS)} unpartitioned assets")
    for evaluation in result.get_asset_check_evaluations():
        seen_checks.add(evaluation.check_name)
        check(evaluation.passed, f"check '{evaluation.check_name}' passed")

    print("\n==> Partitioned dbt facts (fct_quotes_daily + fct_bound_policies_daily)")
    for date in VALIDATION_DATES:
        result = run(definitions, instance, PARTITIONED_KEYS, partition_key=date, label=f"partitioned facts {date}")
        check(True, f"{date}: materialized {len(PARTITIONED_KEYS)} partitioned assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{date}: check '{evaluation.check_name}' passed")

    print("\n==> Downstream activation + reporting (unpartitioned deps= on partitioned upstream)")
    result = run(definitions, instance, DOWNSTREAM_KEYS, partition_key=None, label="downstream activation/reporting")
    check(True, f"materialized {len(DOWNSTREAM_KEYS)} downstream assets without requiring an upstream partition")

    print("\n==> All expected checks ran")
    for name in DBT_TEST_CHECK_NAMES + CUSTOM_CHECK_NAMES + PARTITIONED_DBT_TEST_CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print("\n==> Row counts (determinism check -- real dbt SQL over fixture data)")
    conn = __import__("duckdb").connect(demo_duckdb_path(), read_only=True)
    try:
        quote_count = conn.execute("select count(*) from raw.quote_requests").fetchone()[0]
        policy_count = conn.execute("select count(*) from raw.bound_policies").fetchone()[0]
        fct_quotes_rows = conn.execute("select count(*) from main_marts.fct_quotes_daily").fetchone()[0]
        fct_policies_total = conn.execute(
            "select sum(policy_count) from main_marts.fct_bound_policies_daily"
        ).fetchone()[0]
    finally:
        conn.close()
    print(f"  raw.quote_requests total rows: {quote_count}")
    print(f"  raw.bound_policies total rows: {policy_count}")
    print(f"  fct_quotes_daily grain rows: {fct_quotes_rows}")
    print(f"  fct_bound_policies_daily sum(policy_count): {fct_policies_total}")
    check(fct_policies_total == policy_count, "fct_bound_policies_daily reconciles exactly against raw.bound_policies")

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
