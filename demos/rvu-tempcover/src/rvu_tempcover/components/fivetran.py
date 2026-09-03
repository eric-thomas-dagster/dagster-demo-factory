"""Demo-mode subclass of the native `dagster_fivetran.FivetranAccountComponent`.

Fixes the defect this rebuild exists to correct: the prior build routed
Fivetran ingestion through a home-made `GraphFirstAssetsComponent`
(banned by construction, per `CLAUDE.md`) instead of the real
`dagster-fivetran` integration. Rung 1 of the escalation ladder --
`FivetranAccountComponent` is a native component, not a registry one --
subclassed per rung 3 to add the demo-mode I/O seam
(`templates/demo_mode_pattern.py`).

`FivetranAccountComponent` already follows the workspace-component
convention documented in `CLAUDE.md` and confirmed by reading its source:
`@public` class, a `translation:` field, a `@public get_asset_spec(props)`
override hook, `polling_sensor` (opt-in), `defs_state` + `defs_state_config`,
`StateBackedComponent` inheritance with enumeration happening in
`write_state_to_path` (the state-write path), not at Dagster load time.
Nothing here forks that -- every seam below is additive over the parent:

1. **Discovery (`write_state_to_path`).** Real mode calls
   `self.workspace_resource.fetch_fivetran_workspace_data()`, which hits the
   live Fivetran REST API. Demo mode builds the exact same
   `FivetranWorkspaceData` shape from a fixed, explicit connector list
   (`connectors`, one entry per RVU source table -- the same
   `assets_by_task_key`-shaped mapping table pattern CLAUDE.md calls for)
   using the real `FivetranConnector.from_connector_details` /
   `FivetranDestination.from_destination_details` /
   `FivetranSchemaConfig.from_schema_config_details` classmethods, fed
   literal dicts shaped like the real Fivetran API's JSON responses --
   the same seam `templates/demo_mode_pattern.py` documents: fake only the
   outermost I/O call, keep every downstream code path (spec construction,
   connector grouping, sensor building) real and untouched.

2. **Asset representation (`get_asset_spec`).** The base translator keys
   assets `[schema_name, table_name]` (e.g. `["raw", "quote_requests"]`).
   RVU's asset list names single-segment keys (`raw_quote_requests`, ...),
   so this is exactly the documented override point CLAUDE.md calls out --
   remaps the key and layers on the house-rule-required metadata
   (`owner`, `owner_team`, `tier`, `domain`, `business_impact`,
   `integration_pattern`) from the same `connectors` mapping table. Real
   mode's metadata (row-level column info, connector URL, sync schedule)
   is preserved underneath via `merge_attributes` -- only the key,
   group_name, kinds and house-rule metadata are replaced.

3. **Execution (`execute`).** Real mode calls
   `fivetran.sync_and_poll(context=context)`, the real Fivetran REST sync
   loop, unmodified. Demo mode reads the row count already sitting in the
   demo warehouse's `raw` schema (landed by `demo_data/bootstrap.py`,
   standing in for "Fivetran already synced this source") and reports it
   as materialization metadata, mirroring the shape a real sync's output
   would have.

`polling_sensor: true` is set in `defs.yaml` (default is False per
LEARNINGS.md) so externally-triggered syncs -- Fivetran's own schedule
firing outside of Dagster -- still show up as observations.
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_fivetran import FivetranAccountComponent, FivetranWorkspace
from dagster_fivetran.translator import (
    FivetranConnector,
    FivetranConnectorTableProps,
    FivetranDestination,
    FivetranMetadataSet,
    FivetranSchemaConfig,
    FivetranWorkspaceData,
)
from pydantic import Field

from rvu_tempcover.demo_data.warehouse import demo_duckdb_path

_DEMO_DESTINATION_ID = "rvu_demo_destination"


class RvuFivetranComponent(FivetranAccountComponent):
    """`FivetranAccountComponent` with a demo-mode discovery + sync seam.

    `connectors` is the explicit per-source mapping table: adding RVU's
    next Fivetran-synced source is one more entry here, never a new
    component instance or a new Python file (the scaling test in
    `CLAUDE.md`, "One component instance, many objects").
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Build a fixed connector list and simulate syncs instead of calling "
            "the Fivetran REST API. Set false and supply real credentials in "
            "`workspace:` to run against a live Fivetran account."
        ),
    )
    demo_seed: int = Field(
        default=20260903,
        description="Seed for deterministic synthetic generation -- unused directly (the "
        "fixture data is already static), kept for parity with the other demo components.",
    )
    connectors: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Explicit mapping from the desired Dagster asset key to the Fivetran connector "
            "that syncs it: connector_id, connector_name, service, schema_name, table_name, "
            "group_name, tier, domain, owner_team, description, business_impact. Adding RVU's "
            "next Fivetran-synced source is one more entry here."
        ),
    )

    async def write_state_to_path(self, state_path: Path) -> None:
        """The discovery seam. Real mode calls the live Fivetran API via the
        parent's own `fetch_fivetran_workspace_data()`; demo mode builds the
        identical `FivetranWorkspaceData` shape from `connectors` using the
        real API-response parsing classmethods, so every downstream code
        path (spec construction, connector grouping) is exercised exactly
        as it would be against a real account.
        """
        if not self.demo_mode:
            return await super().write_state_to_path(state_path)

        destination = FivetranDestination.from_destination_details(
            {"id": _DEMO_DESTINATION_ID, "service": "big_query", "config": {"database": "rvu_demo"}}
        )

        connectors_by_id: dict[str, FivetranConnector] = {}
        schema_configs_by_connector_id: dict[str, FivetranSchemaConfig] = {}
        for asset_key, cfg in self.connectors.items():
            connector_id = cfg["connector_id"]
            connectors_by_id[connector_id] = FivetranConnector.from_connector_details(
                {
                    "id": connector_id,
                    "schema": cfg["connector_name"],
                    "service": cfg.get("service", "fivetran"),
                    "group_id": _DEMO_DESTINATION_ID,
                    "status": {"setup_state": "connected", "sync_state": "scheduled"},
                    "paused": False,
                    "succeeded_at": "2026-09-03T07:00:00.000Z",
                    "failed_at": None,
                    "sync_frequency": 1440,
                    "schedule_type": "auto",
                }
            )
            schema_configs_by_connector_id[connector_id] = FivetranSchemaConfig.from_schema_config_details(
                {
                    "schemas": {
                        cfg["schema_name"]: {
                            "enabled": True,
                            "name_in_destination": cfg["schema_name"],
                            "tables": {
                                cfg["table_name"]: {
                                    "enabled": True,
                                    "name_in_destination": cfg["table_name"],
                                    "columns": None,
                                }
                            },
                        }
                    }
                }
            )

        state = FivetranWorkspaceData(
            connectors_by_id=connectors_by_id,
            destinations_by_id={_DEMO_DESTINATION_ID: destination},
            schema_configs_by_connector_id=schema_configs_by_connector_id,
        )
        state_path.write_text(dg.serialize_value(state), encoding="utf-8")

    def get_asset_spec(self, props: FivetranConnectorTableProps) -> dg.AssetSpec:
        """Documented override hook (see module docstring, point 2): remaps
        the base translator's `[schema, table]` key to RVU's single-segment
        asset key and layers on the house-rule-required metadata, both
        driven by `connectors`. Real-mode connectors not present in the
        mapping fall through to the base translator unchanged.
        """
        spec = super().get_asset_spec(props)
        cfg = next(
            (c for c in self.connectors.values() if c["connector_id"] == props.connector_id),
            None,
        )
        if cfg is None:
            return spec

        return spec.replace_attributes(
            key=dg.AssetKey(cfg["asset_key"]),
            group_name=cfg.get("group_name", "ingestion"),
            kinds={"fivetran"},
            owners=[cfg["owner_team"]] if cfg.get("owner_team") else None,
            description=cfg.get("description"),
        ).merge_attributes(
            metadata={
                "owner": cfg.get("owner", "RVU Data Platform"),
                "owner_team": cfg.get("owner_team"),
                "tier": cfg.get("tier"),
                "domain": cfg.get("domain"),
                "integration_pattern": "fivetran_managed_ingestion",
                "demo_mode": self.demo_mode,
                **({"business_impact": cfg["business_impact"]} if cfg.get("business_impact") else {}),
            }
        )

    def execute(
        self, context: dg.AssetExecutionContext, fivetran: FivetranWorkspace
    ) -> Iterable[dg.AssetMaterialization | dg.MaterializeResult]:
        """The sync seam. Real mode delegates to the parent's own
        `sync_and_poll`, the real Fivetran REST sync loop, unmodified. Demo
        mode reads the row count `demo_data/bootstrap.py` already landed in
        the warehouse's `raw` schema (standing in for "Fivetran already
        synced this source") and reports it the way a real sync's output
        would be reported.
        """
        if not self.demo_mode:
            yield from super().execute(context, fivetran)
            return

        cfg_by_asset_key = {cfg["asset_key"]: cfg for cfg in self.connectors.values()}
        for spec in context.assets_def.specs:
            cfg = cfg_by_asset_key.get(spec.key.to_user_string())
            row_count = _raw_row_count(cfg["schema_name"], cfg["table_name"]) if cfg else None
            yield dg.MaterializeResult(
                asset_key=spec.key,
                metadata={
                    "dagster/row_count": row_count,
                    "fivetran/sync_state": "scheduled",
                    "source": dg.MetadataValue.text(
                        "simulated -- set demo_mode: false in defs.yaml to sync via the real Fivetran API"
                    ),
                },
            )


def _raw_row_count(schema: str, table: str) -> int:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        return conn.execute(f"select count(*) from {schema}.{table}").fetchone()[0]
    finally:
        conn.close()
