"""Mock of the Fivetran REST API -- the one network boundary faked.

`FakeFivetranClient` is a drop-in for `dagster_fivetran.FivetranClient`
(returned by `DemoFivetranWorkspace.get_client()`). Every method the real
`FivetranWorkspace`, `FivetranAccountComponent.execute`, and the polling
sensor call -- group/destination/connector discovery, schema config, details,
`sync_and_poll` -- runs unmodified real Dagster code against these
responses; only the HTTP call is replaced.

Synthetic account: one Fivetran destination (Snowflake) and one SQL Server
connector replicating the three client tables.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from dagster_fivetran.types import FivetranOutput

from skygen.demo_data.landing import LANDING_SCHEMA
from skygen.demo_data.tenant_source import COLUMNS, TABLES

CONNECTOR_ID = "client_sqlserver_replica"
GROUP_ID = "skygen_demo_destination"


def _schema_config() -> dict[str, Any]:
    return {
        "schemas": {
            LANDING_SCHEMA: {
                "enabled": True,
                "name_in_destination": LANDING_SCHEMA,
                "tables": {
                    table: {
                        "enabled": True,
                        "name_in_destination": table,
                        "columns": {
                            column: {"name_in_destination": column, "enabled": True}
                            for column in COLUMNS[table]
                        },
                    }
                    for table in TABLES
                },
            }
        }
    }


def _connector_details() -> dict[str, Any]:
    # The last sync completes on the hour -- so an out-of-band sync shows up
    # once an hour for the polling sensor, instead of on every tick.
    completed = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return {
        "id": CONNECTOR_ID,
        "schema": LANDING_SCHEMA,
        "service": "sql_server",
        "group_id": GROUP_ID,
        "paused": False,
        "status": {"setup_state": "connected", "sync_state": "scheduled"},
        "succeeded_at": completed.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
        "failed_at": (completed - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
        "sync_frequency": 60,
        "schedule_type": "auto",
        "config": {},
    }


class FakeFivetranClient:
    def get_groups(self) -> dict[str, Any]:
        return {"items": [{"id": GROUP_ID, "name": "skygen_demo"}]}

    def get_destination_details(self, destination_id: str) -> dict[str, Any]:
        return {"id": destination_id, "service": "snowflake", "config": {"database": "SKYGEN_DEMO"}}

    def list_connectors_for_group(self, group_id: str) -> list[dict[str, Any]]:
        return [_connector_details()]

    def get_schema_config_for_connector(
        self, connector_id: str, raise_on_not_found_error: bool = True
    ) -> dict[str, Any]:
        return _schema_config()

    def get_connector_details(self, connector_id: str) -> dict[str, Any]:
        return _connector_details()

    def sync_and_poll(self, connector_id: str, poll_interval: float = 0, poll_timeout: float | None = None):
        return FivetranOutput(connector_details=_connector_details(), schema_config=_schema_config())
