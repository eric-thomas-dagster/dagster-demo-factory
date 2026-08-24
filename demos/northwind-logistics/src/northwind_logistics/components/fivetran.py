"""Demo-mode subclass of the native `dagster_fivetran.FivetranAccountComponent`.

`FivetranAccountComponent` is a `StateBackedComponent`: it fetches connector
and schema metadata from the real Fivetran API once (`write_state_to_path`)
and builds the asset graph from that cached state (`build_defs_from_state`,
inherited unmodified here). Demo mode fakes only that state fetch, plus the
sync-execution call -- exactly the two places `FivetranAccountComponent`
itself touches the network. Everything in between (asset spec generation,
grouping tables by connector, the multi-asset shape) is the real component's
code, untouched.

There is no equivalent "warehouse write" step to fake the way there is in
`carrier_rate_feed.py` / `shipment_events.py`: in real life, Fivetran lands
rows in the destination warehouse itself, off in Fivetran's own
infrastructure, not through code Dagster runs. So in demo mode this
component writes synthetic rows straight to the local DuckDB file that
stands in for that destination.
"""

from collections.abc import Iterator
from pathlib import Path

import dagster as dg
from dagster_fivetran import FivetranAccountComponent
from dagster_fivetran.translator import (
    FivetranConnector,
    FivetranDestination,
    FivetranSchema,
    FivetranSchemaConfig,
    FivetranTable,
    FivetranWorkspaceData,
)
from pydantic import Field

from northwind_logistics.demo_data.generators import (
    generate_netsuite_gl_entries_frame,
    generate_salesforce_accounts_frame,
    generate_zendesk_tickets_frame,
)
from northwind_logistics.demo_data.state import DEMO_WINDOW_END, DEMO_WINDOW_START
from northwind_logistics.demo_data.warehouse import connect_with_retry, demo_duckdb_path, write_table

_DEMO_DESTINATION_ID = "demo_destination"

# One entry per synthetic connector: (connector_id, service name, destination table).
_DEMO_CONNECTORS = [
    ("demo_salesforce", "salesforce", "salesforce_accounts"),
    ("demo_zendesk", "zendesk", "zendesk_tickets"),
    ("demo_netsuite", "netsuite_suiteanalytics", "netsuite_gl_entries"),
]

_TABLE_GENERATORS = {
    "salesforce_accounts": lambda seed: generate_salesforce_accounts_frame(seed),
    "zendesk_tickets": lambda seed: generate_zendesk_tickets_frame(seed, DEMO_WINDOW_START, DEMO_WINDOW_END),
    "netsuite_gl_entries": lambda seed: generate_netsuite_gl_entries_frame(seed, DEMO_WINDOW_START, DEMO_WINDOW_END),
}


class DemoFivetranAccountComponent(FivetranAccountComponent):
    """`FivetranAccountComponent` that fakes state-fetch and sync in demo mode."""

    demo_mode: bool = Field(
        default=True,
        description=(
            "Serve synthetic connector state and synthetic sync results instead "
            "of calling the Fivetran API. Set false and supply real Fivetran "
            "credentials to sync a live account -- no other code changes required."
        ),
    )
    demo_seed: int = Field(default=20260824, description="Seed for deterministic synthetic generation.")

    async def write_state_to_path(self, state_path: Path) -> None:
        if not self.demo_mode:
            await super().write_state_to_path(state_path)
            return

        connectors_by_id = {}
        schema_configs_by_connector_id = {}
        for connector_id, service, table_name in _DEMO_CONNECTORS:
            connectors_by_id[connector_id] = FivetranConnector(
                id=connector_id,
                name=service,
                service=service,
                group_id=_DEMO_DESTINATION_ID,
                setup_state="connected",
                sync_state="scheduled",
                paused=False,
                succeeded_at="2026-08-24T06:00:00Z",
                failed_at=None,
            )
            schema_configs_by_connector_id[connector_id] = FivetranSchemaConfig(
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

        state = FivetranWorkspaceData(
            connectors_by_id=connectors_by_id,
            destinations_by_id={
                _DEMO_DESTINATION_ID: FivetranDestination(
                    id=_DEMO_DESTINATION_ID, database="NORTHWIND", service="snowflake"
                )
            },
            schema_configs_by_connector_id=schema_configs_by_connector_id,
        )
        state_path.write_text(dg.serialize_value(state), encoding="utf-8")

    def execute(
        self, context: dg.AssetExecutionContext, fivetran
    ) -> Iterator[dg.AssetMaterialization | dg.MaterializeResult]:
        if not self.demo_mode:
            yield from super().execute(context, fivetran)
            return

        conn = connect_with_retry(demo_duckdb_path())
        try:
            for asset_key in context.selected_asset_keys:
                table_name = asset_key.path[-1]
                if table_name not in _TABLE_GENERATORS:
                    raise ValueError(f"No demo generator registered for Fivetran table: {table_name}")
                frame = _TABLE_GENERATORS[table_name](self.demo_seed)
                write_table(conn, schema="raw", table=table_name, df=frame)
                yield dg.MaterializeResult(
                    asset_key=asset_key,
                    metadata={"dagster/row_count": len(frame), "demo_mode": True},
                )
        finally:
            conn.close()
