"""The genuinely-legacy half of the migration-both-states graph.

`raw_dealer_floorplan_feed` and `raw_credit_bureau_pull` are **not** entries
in `DemoFabricWorkspaceComponent`'s `assets_by_item_name` mapping table (see
`defs/fabric_pipelines/defs.yaml`) -- there is no Fabric pipeline behind
either one, mocked or real. SFS's own homegrown scheduler runs the SSIS
package that produces them today, and Dagster never triggers anything for
these two asset keys: they are declared as plain `AssetSpec`s with zero
Dagster-owned compute (Dagster passes bare `AssetSpec`s straight through to
an `AssetsDefinition` with no materialization function -- the core-Dagster
"external asset" pattern `CLAUDE.md`'s "Orchestrating existing workloads"
describes), so their materialization history in the UI is entirely
`AssetObservation` events, never a Dagster-triggered run.

`legacy_scheduler_observer` is a dedicated sensor, separate from the Fabric
workspace component's own polling sensor (which only ever concerns
genuinely-migrated Fabric pipelines -- see `demo_data/external_run_history.py`).
It polls SFS's own (mocked) scheduler run log and reports what already
landed; it never triggers anything.

The lateness check (`defs/checks/raw_dealer_floorplan_feed_lateness.py`)
targets an asset Dagster never materializes, so there's no Dagster-triggered
run for it to ride alongside. Building a checks-only job over a non-
executable asset can't infer a `PartitionsDefinition` in this Dagster version
(confirmed by reading `_infer_and_validate_common_partitions_def`,
dagster==1.13.19 -- see the check module's docstring), so
`legacy_scheduler_observer` evaluates the check directly and reports the
result as an `AssetCheckEvaluation` in the same tick as the observation --
"the check still runs, it just evaluates data Dagster didn't produce" (brief,
Asset checks #3).
"""

import dagster as dg

from stellantis_financial_services.defs.checks.raw_dealer_floorplan_feed_lateness import evaluate_lateness
from stellantis_financial_services.demo_data import legacy_scheduler
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path
from stellantis_financial_services.partitions import DAILY_PARTITIONS, DEALER_FEED_PARTITIONS

_COMMON_METADATA = {
    "integration_pattern": "coexistence",
    "legacy_system_boundary": "homegrown_scheduler_owned",
}

raw_dealer_floorplan_feed = dg.AssetSpec(
    key=dg.AssetKey(["raw_dealer_floorplan_feed"]),
    description=(
        "Per-region dealer floorplan financing feed -- still produced by SFS's own SSIS package "
        "under their homegrown scheduler, not yet cut over to Fabric. Dagster never triggers this; "
        "it only learns of a completed run via legacy_scheduler_observer."
    ),
    group_name="bronze_ingestion",
    kinds={"azure"},
    owners=["team:enterprise-data-management"],
    partitions_def=DEALER_FEED_PARTITIONS,
    metadata={
        **_COMMON_METADATA,
        "owner": "Enterprise Data Management",
        "owner_team": "team:enterprise-data-management",
        "tier": "tier_2",
        "domain": "dealer_floorplan",
    },
)

raw_credit_bureau_pull = dg.AssetSpec(
    key=dg.AssetKey(["raw_credit_bureau_pull"]),
    description=(
        "Daily commercial/consumer credit bureau pull -- still produced by SFS's own SSIS package "
        "under their homegrown scheduler, not yet cut over to Fabric. Dagster never triggers this; "
        "it only learns of a completed run via legacy_scheduler_observer."
    ),
    group_name="bronze_ingestion",
    kinds={"azure"},
    owners=["team:enterprise-data-management"],
    partitions_def=DAILY_PARTITIONS,
    metadata={
        **_COMMON_METADATA,
        "owner": "Enterprise Data Management",
        "owner_team": "team:enterprise-data-management",
        "tier": "tier_2",
        "domain": "credit_underwriting",
    },
)

# Rotation the observer polls through, one per tick -- mirrors the Fabric
# workspace component's own polling sensor shape (demo_data/external_run_history.py).
_ROTATION: list[dict] = [
    {"asset_name": "raw_credit_bureau_pull", "dealer_group": None},
    {"asset_name": "raw_dealer_floorplan_feed", "dealer_group": "midwest"},
    {"asset_name": "raw_dealer_floorplan_feed", "dealer_group": "northeast"},
    {"asset_name": "raw_dealer_floorplan_feed", "dealer_group": "south"},
    {"asset_name": "raw_dealer_floorplan_feed", "dealer_group": "west"},
]


@dg.sensor(
    name="legacy_scheduler_observer",
    minimum_interval_seconds=30,
    description=(
        "Polls SFS's own homegrown scheduler's run log for the two feeds still running as SSIS "
        "packages (raw_dealer_floorplan_feed, raw_credit_bureau_pull). Never triggers a run -- SFS's "
        "own scheduler stays master for these until they're migrated -- only reports what it already "
        "completed, with arrival timing, as AssetObservation events."
    ),
)
def legacy_scheduler_observer(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    try:
        idx = int(context.cursor) if context.cursor else -1
    except ValueError:
        idx = -1
    next_idx = (idx + 1) % len(_ROTATION)
    item = _ROTATION[next_idx]

    event_date = DAILY_PARTITIONS.get_last_partition_key()
    conn = connect_with_retry(demo_duckdb_path())
    try:
        legacy_scheduler.ensure_legacy_data_landed(conn, item["asset_name"], event_date, item["dealer_group"])
        completion = legacy_scheduler.legacy_completion_metadata(conn, item["asset_name"], event_date, item["dealer_group"])
    finally:
        conn.close()

    partition = (
        dg.MultiPartitionKey({"date": event_date, "dealer_group": item["dealer_group"]})
        if item["dealer_group"]
        else event_date
    )
    asset_events: list = [
        dg.AssetObservation(
            asset_key=dg.AssetKey([item["asset_name"]]),
            partition=partition,
            metadata={
                "legacy/source_system": completion["source_system"],
                "legacy/completed_at": completion["completed_at"],
                "legacy/observed_via": "legacy_scheduler_observer",
            },
        )
    ]

    if item["asset_name"] == "raw_dealer_floorplan_feed":
        conn = connect_with_retry(demo_duckdb_path())
        try:
            check_result = evaluate_lateness(conn, event_date, item["dealer_group"])
        finally:
            conn.close()
        asset_events.append(
            dg.AssetCheckEvaluation(
                asset_key=dg.AssetKey(["raw_dealer_floorplan_feed"]),
                check_name="raw_dealer_floorplan_feed_lateness",
                passed=check_result.passed,
                metadata=check_result.metadata,
                description=check_result.description,
                severity=dg.AssetCheckSeverity.WARN,
                blocking=False,
                partition=str(partition),
            )
        )

    return dg.SensorResult(asset_events=asset_events, cursor=str(next_idx))


defs = dg.Definitions(
    assets=[raw_dealer_floorplan_feed, raw_credit_bureau_pull],
    sensors=[legacy_scheduler_observer],
)
