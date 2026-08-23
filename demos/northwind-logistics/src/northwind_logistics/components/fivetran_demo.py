"""Demo-mode Fivetran account component. Read templates/demo_mode_pattern.py first.

`FivetranAccountComponent` is a `StateBackedComponent`: it discovers connectors
and tables by calling the real Fivetran API once, up front, and caches that
discovery as local state (see the state-backed components reference). That
discovery call -- `write_state_to_path` -- is the seam this subclass fakes in
demo mode, alongside `execute`, the sync-time seam the base
`demo_mode_pattern.py` example shows for a plain (non-state-backed) component.
Both are overridden; asset spec generation, the connector selector, and the
YAML schema are all inherited unchanged from `FivetranAccountComponent`.
"""

from pathlib import Path

import dagster as dg
from dagster_fivetran import FivetranAccountComponent
from dagster_fivetran.resources import FivetranWorkspace
from dagster_fivetran.translator import (
    FivetranConnector,
    FivetranConnectorTableProps,
    FivetranDestination,
    FivetranSchema,
    FivetranSchemaConfig,
    FivetranTable,
    FivetranWorkspaceData,
)
from dagster_shared.serdes.serdes import serialize_value
from pydantic import Field

from northwind_logistics.components.warehouse_io import replace_all_rows
from northwind_logistics.demo_data.generators import (
    generate_netsuite_gl_entries,
    generate_salesforce_accounts,
    generate_zendesk_tickets,
)
from northwind_logistics.defs.resources import build_warehouse_resource

_GENERATORS_BY_TABLE = {
    "salesforce_accounts": generate_salesforce_accounts,
    "zendesk_tickets": generate_zendesk_tickets,
    "netsuite_gl_entries": generate_netsuite_gl_entries,
}

_DESCRIPTIONS_BY_TABLE = {
    "salesforce_accounts": "Salesforce accounts -- Northwind's shipping customers and their owners.",
    "zendesk_tickets": (
        "Zendesk support tickets, including the missing-invoice complaints that are "
        "today Northwind's first signal a pipeline broke."
    ),
    "netsuite_gl_entries": "NetSuite general-ledger entries used to reconcile invoice billing.",
}


class DemoFivetranAccountComponent(FivetranAccountComponent):
    """`FivetranAccountComponent` that can serve synthetic connector state and syncs."""

    demo_mode: bool = Field(default=True, description="Serve synthetic connector state and syncs.")
    demo_seed: int = Field(default=20260824, description="Base seed for synthetic generation.")

    async def write_state_to_path(self, state_path: Path) -> None:
        if not self.demo_mode:
            await super().write_state_to_path(state_path)
            return
        state = _synthetic_workspace_data()
        state_path.write_text(serialize_value(state), encoding="utf-8")

    def get_asset_spec(self, props: FivetranConnectorTableProps) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(props)
        table_name = props.table.split(".")[-1]
        return base_spec.replace_attributes(
            key=dg.AssetKey([table_name]),
            group_name="ingestion",
            description=_DESCRIPTIONS_BY_TABLE[table_name],
        )

    def execute(self, context: dg.AssetExecutionContext, fivetran: FivetranWorkspace):
        if not self.demo_mode:
            yield from super().execute(context, fivetran)
            return
        warehouse = build_warehouse_resource()
        for asset_key in context.selected_asset_keys:
            table_name = asset_key.path[-1]
            frame = _GENERATORS_BY_TABLE[table_name](self.demo_seed)
            with warehouse.get_connection() as conn:
                replace_all_rows(conn, schema="raw", table=table_name, frame=frame)
            yield dg.MaterializeResult(
                asset_key=asset_key,
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": True,
                    "source": dg.MetadataValue.text(
                        "synthetic -- set demo_mode: false to sync via a real Fivetran connector"
                    ),
                },
            )


def _synthetic_workspace_data() -> FivetranWorkspaceData:
    """A `FivetranWorkspaceData` snapshot with one connector per SaaS source.

    Shaped exactly like what `FivetranWorkspace.fetch_fivetran_workspace_data()`
    would return from a real account, so `FivetranAccountComponent`'s
    unmodified asset-spec logic has no idea it's reading synthetic state.
    """
    destination = FivetranDestination(id="demo_destination", database="NORTHWIND", service="snowflake")
    connectors = {}
    schema_configs = {}
    for connector_name, table_name in (
        ("salesforce", "salesforce_accounts"),
        ("zendesk", "zendesk_tickets"),
        ("netsuite", "netsuite_gl_entries"),
    ):
        connector_id = f"demo_{connector_name}"
        connectors[connector_id] = FivetranConnector(
            id=connector_id,
            name=connector_name,
            service=connector_name,
            group_id=destination.id,
            setup_state="connected",
            sync_state="scheduled",
            paused=False,
            succeeded_at="2026-08-23T00:00:00Z",
            failed_at=None,
        )
        schema_configs[connector_id] = FivetranSchemaConfig(
            schemas={
                "raw": FivetranSchema(
                    enabled=True,
                    name_in_destination="raw",
                    tables={
                        table_name: FivetranTable(
                            enabled=True,
                            name_in_destination=table_name,
                            columns=None,
                        )
                    },
                )
            }
        )
    return FivetranWorkspaceData(
        connectors_by_id=connectors,
        destinations_by_id={destination.id: destination},
        schema_configs_by_connector_id=schema_configs,
    )
