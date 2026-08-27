"""Demo-mode subclass of the community registry's `FabricWorkspaceComponent`.

Rung 3 of the escalation ladder (`CLAUDE.md`, "Component escalation ladder"):
subclasses the registry component rather than writing one from scratch. Every
seam below is additive over the parent -- asset construction still goes
through the parent's documented `get_asset_spec(props)` override hook, state
still gets written by the parent's `write_state_to_path` / `defs_state_config`
machinery (`StateBackedComponent`), and real mode still calls the parent's own
`_trigger_item_run` to hit the real Fabric REST API. Nothing is forked.

Three gaps the base component has, each closed the rung-3 way (subclass,
don't rewrite):

1. **Live discovery.** `_list_items()` is the single method that crosses the
   network (`GET /workspaces/{id}/items`). In demo mode it returns a fixed
   list built from `assets_by_item_name` instead of calling Fabric -- the
   same seam `templates/demo_mode_pattern.py` documents for any component.

2. **Per-item asset keys / deps / partitions / metadata.** The base
   component's `get_asset_spec(props)` hook exists precisely for this
   (see its docstring), but the base `_apply_translation` only calls it when
   a `translation:` callable is also configured -- a gap in the base
   component (see `component-feedback/`). Rather than requiring every
   defs.yaml author to configure a no-op `translation:` just to unlock the
   hook that already exists for this purpose, `build_defs_from_state` here
   calls `self.get_asset_spec(props)` directly, so the documented override
   point works the way its own docstring says it does. `assets_by_item_name`
   -- one entry per Fabric pipeline, each carrying `key` / `deps` /
   `partitions` / `kinds` / metadata -- is the explicit mapping table the
   brief calls for, the same shape as `assets_by_task_key` on the Databricks
   workspace component (github.com/eric-thomas-dagster/databricks-workspace-
   bundles-demo, `defs/workspace_us/defs.yaml`). Adding SFS's 700th package
   is one more entry in this table, not a new Python file.

3. **The polling sensor never actually builds.** The base component declares
   a `polling_sensor` field (alias `generate_sensor`) and documents it in its
   README, but `StateBackedComponent.build_defs` only ever calls
   `build_defs_from_state` -- there is no code path in the base component
   that constructs a `SensorDefinition`, with or without the flag set. This
   was the gap that sank the three prior builds of this demo (see
   `state/ledger.json`'s note on PR #10): setting `generate_sensor: true`
   changed nothing, because the field is read nowhere. `build_defs_from_state`
   below builds the sensor itself, gated on the same flag, so the
   already-documented config surface finally does what it says. Recorded in
   `component-feedback/` as the suggested fix upstream.

Per `templates/demo_mode_pattern.py`: asset keys, specs, partitions, checks,
the sensor, and the YAML schema are identical whether `demo_mode` is true or
false. Only `_list_items` (discovery) and the compute function's I/O boundary
differ.
"""

from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

import dagster as dg
from pydantic import Field

from stellantis_financial_services.components.fabric_workspace.component import (
    FabricObjectProps,
    FabricWorkspaceComponent,
)
from stellantis_financial_services.demo_data import generators, legacy_scheduler, transforms
from stellantis_financial_services.demo_data.external_run_history import EXTERNALLY_TRIGGERED_ITEMS
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path, upsert_partition
from stellantis_financial_services.partitions import (
    DAILY_PARTITIONS,
    DEALER_FEED_PARTITIONS,
    DEALER_GROUP_ROLLUP_MAPPING,
)

_PARTITIONS_BY_NAME: dict[Optional[str], Any] = {
    None: None,
    "daily": DAILY_PARTITIONS,
    "dealer_floorplan": DEALER_FEED_PARTITIONS,
}


# --- bronze: synthetic vendor-file generation, written to the raw schema ---

def _h_raw_loan_originations(conn, event_date, seed, dealer_group):
    frame = generators.generate_loan_originations_frame(event_date, seed)
    upsert_partition(
        conn, "raw", "raw_loan_originations", frame, {"origination_date": event_date},
        ddl_columns={
            "loan_id": "VARCHAR", "origination_date": "VARCHAR", "dealer_id": "VARCHAR",
            "dealer_group": "VARCHAR", "borrower_id": "VARCHAR", "vehicle_model": "VARCHAR",
            "principal_amount": "DOUBLE", "apr": "DOUBLE", "term_months": "BIGINT", "state": "VARCHAR",
        },
    )
    return len(frame)


def _h_raw_lease_originations(conn, event_date, seed, dealer_group):
    frame = generators.generate_lease_originations_frame(event_date, seed)
    upsert_partition(
        conn, "raw", "raw_lease_originations", frame, {"origination_date": event_date},
        ddl_columns={
            "lease_id": "VARCHAR", "origination_date": "VARCHAR", "dealer_id": "VARCHAR",
            "dealer_group": "VARCHAR", "borrower_id": "VARCHAR", "vehicle_model": "VARCHAR",
            "capitalized_cost": "DOUBLE", "residual_value": "DOUBLE", "money_factor": "DOUBLE",
            "term_months": "BIGINT", "state": "VARCHAR",
        },
    )
    return len(frame)


def _h_raw_payment_transactions(conn, event_date, seed, dealer_group):
    frame = generators.generate_payment_transactions_frame(event_date, seed)
    upsert_partition(
        conn, "raw", "raw_payment_transactions", frame, {"payment_date": event_date},
        ddl_columns={
            "payment_id": "VARCHAR", "account_id": "VARCHAR", "dealer_group": "VARCHAR",
            "payment_date": "VARCHAR", "amount_due": "DOUBLE", "amount_paid": "DOUBLE",
            "days_past_due": "BIGINT",
        },
    )
    return len(frame)


# --- silver/gold/reporting: SQL transforms over already-landed tables ---

def _ignore_seed_and_dealer_group(fn):
    return lambda conn, event_date, seed, dealer_group: fn(conn, event_date)


def _h_dim_dealer(conn, event_date, seed, dealer_group):
    """dim_dealer rolls up all four dealer_group partitions of the still-
    legacy floorplan feed for one date -- the boundary crossing itself. This
    is where the demo needs the legacy system's shared storage to actually
    have data, the same way it would read a shared lakehouse table in
    production regardless of which system landed it (see
    `demo_data/legacy_scheduler.py`)."""
    for group in generators.DEALER_GROUPS:
        legacy_scheduler.ensure_legacy_data_landed(conn, "raw_dealer_floorplan_feed", event_date, group)
    return transforms.dim_dealer(conn, event_date)


def _h_dim_borrower(conn, event_date, seed, dealer_group):
    """dim_borrower joins conformed originations against the still-legacy
    credit bureau pull -- the second boundary crossing."""
    legacy_scheduler.ensure_legacy_data_landed(conn, "raw_credit_bureau_pull", event_date)
    return transforms.dim_borrower(conn, event_date)


_HANDLERS = {
    "raw_loan_originations": _h_raw_loan_originations,
    "raw_lease_originations": _h_raw_lease_originations,
    "raw_payment_transactions": _h_raw_payment_transactions,
    "stg_loan_originations": _ignore_seed_and_dealer_group(transforms.stg_loan_originations),
    "stg_lease_originations": _ignore_seed_and_dealer_group(transforms.stg_lease_originations),
    "stg_payment_transactions": _ignore_seed_and_dealer_group(transforms.stg_payment_transactions),
    "stg_delinquency_events": _ignore_seed_and_dealer_group(transforms.stg_delinquency_events),
    "dim_dealer": _h_dim_dealer,
    "dim_borrower": _h_dim_borrower,
    "fact_loan_portfolio": _ignore_seed_and_dealer_group(transforms.fact_loan_portfolio),
    "fact_delinquency_snapshot": _ignore_seed_and_dealer_group(transforms.fact_delinquency_snapshot),
    "abs_pool_eligibility": _ignore_seed_and_dealer_group(transforms.abs_pool_eligibility),
    "gl_reconciliation_summary": _ignore_seed_and_dealer_group(transforms.gl_reconciliation_summary),
    "customer_360": _ignore_seed_and_dealer_group(transforms.customer_360),
    "powerbi_portfolio_dashboard_refresh": _ignore_seed_and_dealer_group(transforms.refresh_dashboard),
}


class DemoFabricWorkspaceComponent(FabricWorkspaceComponent):
    """`FabricWorkspaceComponent` with a demo-mode discovery seam and an
    explicit per-item asset mapping. See module docstring for the three gaps
    this closes over the base component.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Discover a fixed item list and simulate pipeline runs instead of "
            "calling the Fabric REST API. Set false and supply workspace "
            "credentials to run against a real Fabric workspace."
        ),
    )
    demo_seed: int = Field(
        default=20260826,
        description="Seed for deterministic synthetic generation -- repeat demo runs must not drift.",
    )
    assets_by_item_name: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Explicit mapping from Fabric pipeline display name to the Dagster asset it "
            "becomes: key, deps, partitions, group_name, kinds, owners, description, "
            "metadata. Adding SFS's next migrated package is one more entry here, not a "
            "new Python file -- the same shape as assets_by_task_key on the Databricks "
            "workspace component."
        ),
    )

    def _list_items(self) -> list[dict]:
        """The network seam. Real mode calls the Fabric REST API; demo mode
        returns a fixed item list built from `assets_by_item_name`."""
        if not self.demo_mode:
            return super()._list_items()
        return [
            {
                "id": f"fabric-item-{name}",
                "displayName": name,
                "type": "DataPipeline",
                "description": f"Fabric Data Pipeline: {name}",
                "workspaceId": self.workspace.workspace_id,
            }
            for name in self.assets_by_item_name
        ]

    def get_asset_spec(self, props: FabricObjectProps) -> dg.AssetSpec:
        """Builds the full AssetSpec for one Fabric pipeline item from its
        `assets_by_item_name` entry -- the documented override hook (see the
        base class docstring), called directly here rather than through
        `_apply_translation` (see module docstring, gap 2)."""
        cfg = (props.extra or {}).get("cfg")
        if cfg is None:
            return super().get_asset_spec(props)

        key_parts = cfg["key"] if isinstance(cfg["key"], list) else [cfg["key"]]
        deps = [self._build_dep(d) for d in cfg.get("deps", [])]
        partitions_def = _PARTITIONS_BY_NAME[cfg.get("partitions")]

        metadata = dict(cfg.get("metadata") or {})
        metadata.update(
            {
                "fabric_item_id": (props.extra or {}).get("item_id"),
                "fabric_item_type": (props.extra or {}).get("item_type"),
                "workspace_id": props.workspace_id,
                "demo_mode": self.demo_mode,
            }
        )

        freshness_policy = None
        if cfg.get("freshness_fail_hours"):
            freshness_policy = dg.FreshnessPolicy.time_window(
                fail_window=timedelta(hours=cfg["freshness_fail_hours"]),
                warn_window=(
                    timedelta(hours=cfg["freshness_warn_hours"]) if cfg.get("freshness_warn_hours") else None
                ),
            )

        return dg.AssetSpec(
            key=dg.AssetKey(key_parts),
            deps=deps,
            description=cfg.get("description"),
            group_name=cfg.get("group_name", self.group_name),
            kinds=set(cfg.get("kinds") or ["fabric"]),
            owners=cfg.get("owners"),
            tags=cfg.get("tags"),
            metadata=metadata,
            partitions_def=partitions_def,
            freshness_policy=freshness_policy,
            automation_condition=(dg.AutomationCondition.eager() if cfg.get("automation_eager") else None),
        )

    def _build_dep(self, dep_cfg) -> dg.AssetDep:
        if isinstance(dep_cfg, str):
            return dg.AssetDep(dg.AssetKey([dep_cfg]))
        key_parts = dep_cfg["key"] if isinstance(dep_cfg["key"], list) else [dep_cfg["key"]]
        partition_mapping = (
            DEALER_GROUP_ROLLUP_MAPPING if dep_cfg.get("partition_mapping") == "dealer_group_rollup" else None
        )
        return dg.AssetDep(dg.AssetKey(key_parts), partition_mapping=partition_mapping)

    def build_defs_from_state(self, context: dg.ComponentLoadContext, state_path: Optional[Path]) -> dg.Definitions:
        if state_path is None or not state_path.exists():
            return dg.Definitions()
        import json

        state: dict[str, Any] = json.loads(state_path.read_text())

        assets = []
        for row in state.get("pipelines", []):
            name = row.get("displayName")
            cfg = self.assets_by_item_name.get(name)
            if cfg is None:
                continue
            assets.append(self._build_pipeline_asset(row, cfg))

        defs_kwargs: dict[str, Any] = {"assets": assets}
        if self.polling_sensor:
            defs_kwargs["sensors"] = [self._build_polling_sensor()]
        return dg.Definitions(**defs_kwargs)

    def _build_pipeline_asset(self, row: dict, cfg: dict):
        item_id = row.get("id")
        item_name = row.get("displayName")
        props = FabricObjectProps(
            object_kind="data_pipeline",
            object_name=item_name,
            workspace_id=self.workspace.workspace_id,
            extra={"item_id": item_id, "item_type": row.get("type"), "cfg": cfg},
        )
        spec = self.get_asset_spec(props)
        key_parts = cfg["key"] if isinstance(cfg["key"], list) else [cfg["key"]]
        op_name = key_parts[-1]
        _self = self

        @dg.multi_asset(specs=[spec], name=op_name)
        def _asset(context: dg.AssetExecutionContext):
            return _self._run_pipeline_item(context, item_id, item_name)

        return _asset

    def _run_pipeline_item(self, context: dg.AssetExecutionContext, item_id: str, item_name: str):
        """Trigger-and-observe compute body. Real mode triggers and polls the
        real Fabric pipeline job (`FabricWorkspaceComponent._trigger_item_run`,
        unmodified). Demo mode runs the matching local synthetic-data handler
        with the same run/complete lifecycle a real trigger would have."""
        if not self.demo_mode:
            result = self._trigger_item_run(item_id, "DataPipeline", context.log)
            return dg.MaterializeResult(metadata={"fabric/job_status": result.get("status", "Unknown")})

        partition_key = context.partition_key
        if isinstance(partition_key, dg.MultiPartitionKey):
            event_date = partition_key.keys_by_dimension["date"]
            dealer_group = partition_key.keys_by_dimension["dealer_group"]
        else:
            event_date = partition_key
            dealer_group = None

        handler = _HANDLERS[item_name]
        conn = connect_with_retry(demo_duckdb_path())
        try:
            row_count = handler(conn, event_date, self.demo_seed, dealer_group)
        finally:
            conn.close()

        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": row_count,
                "fabric/job_status": "Completed",
                "as_of_date": event_date,
                "source": dg.MetadataValue.text(
                    "simulated -- set demo_mode: false in defs.yaml to trigger the real Fabric pipeline"
                ),
            }
        )

    def _build_polling_sensor(self) -> dg.SensorDefinition:
        """Detects Fabric pipeline runs Dagster didn't trigger and emits
        `AssetObservation` events -- the money-shot answer to "what happens
        when it wasn't Dagster that started it." Demo mode rotates through a
        fixed, deterministic list (`EXTERNALLY_TRIGGERED_ITEMS`); real mode
        would poll the same Fabric run-history endpoint `_trigger_item_run`
        already polls, filtered to runs this component didn't itself trigger.
        """
        _self = self

        @dg.sensor(
            name="fabric_pipelines_external_run_observer",
            minimum_interval_seconds=30,
            description=(
                "Polls for Fabric pipeline runs Dagster did not trigger -- SFS's own "
                "scheduler, or an operator running a package by hand mid-migration -- "
                "and emits AssetObservation events so they land in the same lineage graph."
            ),
        )
        def _sensor(context: dg.SensorEvaluationContext):
            if not _self.demo_mode:
                # Real mode: poll the Fabric run-history endpoint for job instances
                # this component did not itself trigger, filtered by `since` cursor.
                return dg.SkipReason("Real-mode Fabric run-history polling not exercised in this demo.")

            events = EXTERNALLY_TRIGGERED_ITEMS
            try:
                idx = int(context.cursor) if context.cursor else -1
            except ValueError:
                idx = -1
            next_idx = (idx + 1) % len(events)
            event = events[next_idx]

            latest_key = DAILY_PARTITIONS.get_last_partition_key()
            if "dealer_group" in event:
                partition = dg.MultiPartitionKey({"date": latest_key, "dealer_group": event["dealer_group"]})
            else:
                partition = latest_key

            observation = dg.AssetObservation(
                asset_key=dg.AssetKey(event["asset_key"]),
                partition=partition,
                metadata={
                    "fabric/triggered_by": event["triggered_by"],
                    "fabric/observed_via": "polling_sensor",
                },
            )
            return dg.SensorResult(asset_events=[observation], cursor=str(next_idx))

        return _sensor
