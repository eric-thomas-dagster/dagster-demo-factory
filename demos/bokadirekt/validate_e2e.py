"""End-to-end validation of the Bokadirekt demo ("Ship the Second Deployment").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- two of this
project's seven assets carry partitions, so this script is the harness
`scripts/validate_demo.sh` requires instead. It proves, in dependency order:

1. `raw_bookings_feed` observes cleanly (an observation run, not a
   materialization -- it's a `SourceAsset`).
2. The unpartitioned dbt staging + entity layers (`stg_bookings`,
   `stg_specialists`, `dim_specialists`) materialize, running real dbt SQL
   over the seeded `raw.raw_bookings` / `raw.raw_specialists` tables --
   including the dbt-native tests, the blocking `stg_bookings_completeness`
   check, and the warning `stg_specialists_schema_pii` check.
3. `fct_bookings_daily` materializes across two partition dates, including
   the blocking `fct_bookings_daily_completeness` check. Per the same
   convention used by demos/partners-fcu, Dagster's partition bookkeeping
   for this asset is independent of dbt's own execution grain -- the dbt
   SQL is a small full-refresh transform over the whole accumulated raw
   window, not an incremental build scoped to one partition.
4. `specialist_utilization_daily` -- a stubbed Python asset -- materializes
   across both dates, downstream of both `dim_specialists` (unpartitioned)
   and `fct_bookings_daily` (partitioned) via plain `deps=` edges.

Per the brief and house rules this is an always-green demo: no planted
anomaly, no failure demonstration. Every check computed here is real,
against real (if small) seeded data -- not a hardcoded pass.

Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from bokadirekt.definitions import defs as defs_lazy  # noqa: E402
from bokadirekt.demo_data.warehouse import demo_duckdb_path  # noqa: E402

OBSERVABLE_KEYS = [dg.AssetKey("raw_bookings_feed")]

UNPARTITIONED_DBT_KEYS = [
    dg.AssetKey(["staging", "stg_bookings"]),
    dg.AssetKey(["staging", "stg_specialists"]),
    dg.AssetKey(["marts", "dim_specialists"]),
]

PARTITIONED_KEYS = [
    dg.AssetKey(["marts", "fct_bookings_daily"]),
]

ROLLUP_KEYS = [
    dg.AssetKey("specialist_utilization_daily"),
]

ALL_ASSET_COUNT = 7  # raw/raw_specialists (implicit dbt source) + the 6 above

# The daily partition definition starts 2026-08-01; today's day itself is
# never a valid partition key (DailyPartitionsDefinition rejects it), so
# these stay fixed historical dates rather than "yesterday relative to now".
VALIDATION_DATES = ["2026-08-10", "2026-08-11"]

DBT_TEST_CHECK_NAMES = (
    "not_null_stg_bookings_booking_id",
    "unique_stg_bookings_booking_id",
    "not_null_stg_bookings_specialist_id",
    "not_null_stg_bookings_booking_date",
    "not_null_stg_specialists_specialist_id",
    "unique_stg_specialists_specialist_id",
    "not_null_stg_specialists_region",
    "not_null_dim_specialists_specialist_id",
    "unique_dim_specialists_specialist_id",
)
CUSTOM_CHECK_NAMES = (
    "stg_bookings_completeness",
    "stg_specialists_schema_pii",
    "fct_bookings_daily_completeness",
)
PARTITIONED_DBT_TEST_CHECK_NAMES = (
    "unique_fct_bookings_daily_booking_date",
    "not_null_fct_bookings_daily_booking_date",
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
        raise RuntimeError(f"run failed: {label} partition={partition_key}")
    check(True, f"ran {label}" + (f" partition={partition_key}" if partition_key else ""))
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

    print("\n==> raw_bookings_feed (observation, not materialization)")
    run(definitions, instance, OBSERVABLE_KEYS, None, "raw_bookings_feed observation", seen_checks)

    print("\n==> Unpartitioned dbt chain (staging + dim_specialists)")
    run(definitions, instance, UNPARTITIONED_DBT_KEYS, None, "staging + dim_specialists", seen_checks)

    print("\n==> Partitioned dbt fact (fct_bookings_daily)")
    for date in VALIDATION_DATES:
        run(definitions, instance, PARTITIONED_KEYS, date, f"fct_bookings_daily {date}", seen_checks)

    print("\n==> Python rollup (specialist_utilization_daily, stubbed)")
    for date in VALIDATION_DATES:
        run(definitions, instance, ROLLUP_KEYS, date, f"specialist_utilization_daily {date}", seen_checks)

    print("\n==> All expected checks ran")
    for name in DBT_TEST_CHECK_NAMES + CUSTOM_CHECK_NAMES + PARTITIONED_DBT_TEST_CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print("\n==> Axis 1: real I/O landed in the demo-mode DuckDB file")
    conn = __import__("duckdb").connect(demo_duckdb_path(), read_only=True)
    try:
        booking_count = conn.execute("select count(*) from raw.raw_bookings").fetchone()[0]
        specialist_count = conn.execute("select count(*) from raw.raw_specialists").fetchone()[0]
        stg_bookings_count = conn.execute("select count(*) from main_staging.stg_bookings").fetchone()[0]
        fct_total = conn.execute("select sum(booking_count) from main_marts.fct_bookings_daily").fetchone()[0]
        dim_specialists_count = conn.execute("select count(*) from main_marts.dim_specialists").fetchone()[0]
    finally:
        conn.close()
    print(f"  raw.raw_bookings total rows: {booking_count}")
    print(f"  raw.raw_specialists total rows: {specialist_count}")
    print(f"  staging.stg_bookings rows: {stg_bookings_count}")
    print(f"  fct_bookings_daily sum(booking_count): {fct_total}")
    print(f"  dim_specialists rows: {dim_specialists_count}")
    check(booking_count > 0 and specialist_count > 0, "both raw tables hold real seeded rows")
    check(stg_bookings_count == booking_count, "stg_bookings reconciles exactly against raw.raw_bookings")
    check(dim_specialists_count == specialist_count, "dim_specialists holds one row per specialist")

    print()
    if _failures:
        print(f"FAILED: {len(_failures)} assertion(s) did not hold:")
        for f in _failures:
            print(f"  - {f}")
        return 1

    print("PASSED: all assets materialized/observed, all checks evaluated and green, real I/O confirmed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
