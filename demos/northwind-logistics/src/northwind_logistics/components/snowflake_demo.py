"""Demo-mode Snowflake resource. Read templates/demo_mode_pattern.py before editing.

Subclasses `SnowflakeResource` and overrides only `get_connection` -- the
single method that opens the network connection. Every asset built against
this resource runs the real `dagster-snowflake` code path once `demo_mode`
is false and real credentials are supplied; nothing else about the resource
changes.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Union

import duckdb
from dagster._core.storage.event_log.sql_event_log import SqlDbConnection
from dagster_snowflake import SnowflakeResource
from pydantic import Field
from snowflake import connector as snowflake_connector


class DemoSnowflakeResource(SnowflakeResource):
    """`SnowflakeResource` that redirects warehouse I/O to a local DuckDB file.

    This is the seam the demo's credibility rests on: flip `demo_mode: false`
    and supply real Snowflake credentials, and every asset built against this
    resource runs against the prospect's actual warehouse with no other
    change.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Redirect warehouse I/O to a local DuckDB file instead of Snowflake. "
            "Set false and supply real credentials to run against a live account."
        ),
    )
    demo_duckdb_path: str = Field(
        default="demo_data/warehouse.duckdb",
        description="Path to the DuckDB file standing in for Snowflake, relative to the project root.",
    )

    @contextmanager
    def get_connection(
        self, raw_conn: bool = True
    ) -> Iterator[Union[SqlDbConnection, snowflake_connector.SnowflakeConnection]]:
        if not self.demo_mode:
            with super().get_connection(raw_conn=raw_conn) as conn:
                yield conn
            return

        duckdb_path = Path(self.demo_duckdb_path)
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(duckdb_path))
        try:
            yield conn
        finally:
            conn.close()
