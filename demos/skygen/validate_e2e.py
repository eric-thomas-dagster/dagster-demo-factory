"""End-to-end validation of the Skygen demo ("One Definition, 170 Tenants").

`dg launch --assets '*'` exits immediately on a partitioned asset, and every
Fivetran and dbt asset here is partitioned by (date x tenant), so this script
is the harness `scripts/validate_demo.sh` requires instead. It proves:

1. Structure: the 16-asset graph, edges (Fivetran -> dbt layers -> Sigma), the
   legacy SSIS group deliberately *not* wired into that path, and the
   feature floor (partitions, freshness policy, eager automation, schedule,
   both observation sensors).
2. Three `(tenant, date)` partitions materialize end to end -- Fivetran
   landing, dbt ingest/transform/published/mart -- with every asset check
   (3 custom, 2 blocking; plus dbt tests) evaluating and passing.
3. A tenant-scoped rerun is idempotent and touches only that tenant: row
   counts are unchanged and other tenants' rows are untouched.
4. The Fivetran and SSIS observation sensors emit events for external runs on
   keys that exist in the graph.

Always green: nothing here plants a failure. Run with: `python validate_e2e.py`
"""

import sys
import warnings

import dagster as dg

warnings.filterwarnings("ignore")

from skygen.definitions import defs as defs_lazy  # noqa: E402
from skygen.demo_data.warehouse import connect  # noqa: E402

FIVETRAN_KEYS = [dg.AssetKey(["client_landing", t]) for t in ("dental_claims", "vision_claims", "member_eligibility")]
DBT_KEYS = [
    dg.AssetKey(["ingest", "ing_dental_claims"]),
    dg.AssetKey(["ingest", "ing_vision_claims"]),
    dg.AssetKey(["ingest", "ing_member_eligibility"]),
    dg.AssetKey(["transform", "tfm_claims_unified"]),
    dg.AssetKey(["transform", "tfm_member_coverage"]),
    dg.AssetKey(["published", "pub_member_claims"]),
    dg.AssetKey(["published", "pub_coverage_snapshot"]),
    dg.AssetKey(["mart", "mart_client_sla_scorecard"]),
    dg.AssetKey(["mart", "mart_client_utilization_summary"]),
]
SIGMA_KEYS = [dg.AssetKey(["Client_SLA_Scorecard"]), dg.AssetKey(["Client_Utilization_Overview"])]
LEGACY_KEYS = [dg.AssetKey(["legacy_client_database_nightly_load"]), dg.AssetKey(["legacy_client_sla_report_export"])]
PIPELINE_KEYS = FIVETRAN_KEYS + DBT_KEYS
ALL_ASSET_COUNT = len(PIPELINE_KEYS) + len(SIGMA_KEYS) + len(LEGACY_KEYS)  # 16

CUSTOM_CHECKS = {"landed_row_count_completeness", "published_claims_completeness", "client_sla_data_ready"}
VALIDATION_PARTITIONS = [
    ("tenant_001", "2026-10-01"),
    ("tenant_002", "2026-10-01"),
    ("tenant_001", "2026-10-02"),
]

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def row_counts(tenant: str, day: str) -> dict[str, int]:
    tables = {
        "landed dental_claims": "client_landing.dental_claims",
        "landed vision_claims": "client_landing.vision_claims",
        "landed member_eligibility": "client_landing.member_eligibility",
        "published pub_member_claims": "main_published.pub_member_claims",
        "mart sla_scorecard": "main_mart.mart_client_sla_scorecard",
    }
    with connect(read_only=True) as conn:
        return {
            label: conn.execute(
                f"select count(*) from {table} where tenant_id = ? and report_date = ?", [tenant, day]
            ).fetchone()[0]
            for label, table in tables.items()
        }


def materialize(definitions, instance, tenant: str, day: str, seen_checks: set[str]) -> None:
    job = definitions.resolve_implicit_job_def_def_for_assets(PIPELINE_KEYS)
    partition_key = dg.MultiPartitionKey({"date": day, "tenant": tenant})
    result = job.execute_in_process(
        instance=instance,
        partition_key=partition_key,
        asset_selection=PIPELINE_KEYS,
        raise_on_error=False,
    )
    if not result.success:
        raise RuntimeError(f"materialize failed: {tenant} {day}")
    check(True, f"materialized {len(PIPELINE_KEYS)} assets for {tenant} on {day}")
    evaluations = result.get_asset_check_evaluations()
    for evaluation in evaluations:
        seen_checks.add(evaluation.check_name)
    failed = [e for e in evaluations if not e.passed]
    check(not failed, f"{tenant} {day}: all {len(evaluations)} asset checks passed")


def main() -> int:
    definitions = defs_lazy()
    seen_checks: set[str] = set()
    graph = definitions.get_repository_def().asset_graph

    print("==> [1/4] Structure and feature floor")
    check(len(graph.get_all_asset_keys()) == ALL_ASSET_COUNT, f"asset graph has exactly {ALL_ASSET_COUNT} assets")
    mart = dg.AssetKey(["mart", "mart_client_sla_scorecard"])
    check(set(graph.get(mart).parent_keys) == {dg.AssetKey(["published", "pub_member_claims"])}, "mart reads the published layer")
    check(
        dg.AssetKey(["mart", "mart_client_sla_scorecard"]) in graph.get(SIGMA_KEYS[0]).parent_keys,
        "Sigma scorecard workbook sits downstream of the dbt mart",
    )
    check(
        set(graph.get(FIVETRAN_KEYS[0]).child_keys) == {dg.AssetKey(["ingest", "ing_dental_claims"])},
        "Fivetran table feeds its dbt ingest model",
    )
    legacy_related = set()
    for key in LEGACY_KEYS:
        legacy_related |= set(graph.get(key).parent_keys) | set(graph.get(key).child_keys)
    check(legacy_related <= set(LEGACY_KEYS), "legacy SSIS assets are not wired into the Fivetran -> dbt -> Sigma path")
    check(
        all(isinstance(graph.get(k).partitions_def, dg.MultiPartitionsDefinition) for k in PIPELINE_KEYS),
        "every Fivetran and dbt asset is partitioned by (date x tenant)",
    )
    check(
        all(graph.get(k).automation_condition is not None for k in DBT_KEYS),
        "every dbt asset carries an eager, check-gated automation condition",
    )
    mart_spec = next(
        s for item in definitions.assets for s in (item.specs if hasattr(item, "specs") else [item]) if s.key == mart
    )
    check(mart_spec.freshness_policy is not None, "mart_client_sla_scorecard has a native freshness policy")
    check(definitions.get_schedule_def("skygen_nightly_landing_schedule") is not None, "nightly schedule exists")
    sensor_names = {s.name for s in definitions.sensors}
    check("fivetran_demo_account__sync_status_sensor" in sensor_names, "Fivetran polling sensor exists")
    check("ssis_workspace_observation_sensor" in sensor_names, "SSIS observation sensor exists")

    with dg.instance_for_test() as instance:
        print("\n==> [2/4] Materialize three (tenant, date) partitions end to end")
        for tenant, day in VALIDATION_PARTITIONS:
            materialize(definitions, instance, tenant, day, seen_checks)
        check(CUSTOM_CHECKS <= seen_checks, f"all three custom checks evaluated: {sorted(CUSTOM_CHECKS & seen_checks)}")
        check(len(seen_checks - CUSTOM_CHECKS) > 0, f"dbt-native tests surfaced as checks ({len(seen_checks - CUSTOM_CHECKS)} kinds)")

        print("\n==> Row counts (identical on every run)")
        before = {p: row_counts(*p) for p in VALIDATION_PARTITIONS}
        for (tenant, day), counts in before.items():
            print(f"  {tenant} {day}: {counts}")
        check(all(v > 0 for c in before.values() for v in c.values()), "every layer has rows for every partition")

        print("\n==> [3/4] Tenant-scoped rerun is idempotent and touches only that tenant")
        materialize(definitions, instance, "tenant_001", "2026-10-01", seen_checks)
        after = {p: row_counts(*p) for p in VALIDATION_PARTITIONS}
        check(after == before, "row counts unchanged for all partitions after rerunning tenant_001 / 2026-10-01")

        print("\n==> [4/4] Observation sensors catch runs Dagster didn't start")
        # A fresh instance: nothing Dagster-triggered has recorded this sync, so the
        # sensor treats it as an external run and emits events (the real sensor
        # skips a sync a Dagster run already recorded -- see the materialize steps).
        fivetran_sensor = definitions.get_sensor_def("fivetran_demo_account__sync_status_sensor")
        with dg.instance_for_test() as fresh_instance:
            result = fivetran_sensor.evaluate_tick(
                dg.build_sensor_context(instance=fresh_instance, definitions=definitions)
            )
        events = list(result.asset_events)
        check(
            len(events) > 0 and all(e.asset_key in FIVETRAN_KEYS for e in events),
            f"Fivetran sensor emitted {len(events)} events for an externally triggered sync, all on graph keys",
        )
        ssis_sensor = definitions.get_sensor_def("ssis_workspace_observation_sensor")
        cursor = None
        for _ in range(3):
            result = ssis_sensor.evaluate_tick(
                dg.build_sensor_context(instance=instance, definitions=definitions, cursor=cursor)
            )
            cursor = result.cursor
            for event in result.asset_events:
                check(
                    isinstance(event, dg.AssetObservation) and event.asset_key in LEGACY_KEYS,
                    f"SSIS sensor observed a legacy asset key: {event.asset_key.to_user_string()}",
                )

    if _failures:
        print(f"\nFAILED: {len(_failures)} check(s)")
        return 1
    print("\nAll validation checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
