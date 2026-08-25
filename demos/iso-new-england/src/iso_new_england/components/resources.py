"""Demo-mode resource swap for the Postgres landing zone.

Follows the "Variant: components whose seam is a resource rather than a
method" pattern from `templates/demo_mode_pattern.py`. Subclasses the
community-registry `postgres_resource` component's `PostgresResource`
unmodified apart from the connection seam and demo-friendly defaults, so
flipping `demo_mode: false` and supplying real Postgres credentials is the
entire migration to ISO-NE's actual Postgres landing zone -- no other code
changes required.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from pydantic import Field

from iso_new_england.components.oracle_resource.component import OracleResource
from iso_new_england.components.postgres_resource.component import PostgresResource
from iso_new_england.demo_data.warehouse import connect_with_retry, demo_duckdb_path


class DemoPostgresResource(PostgresResource):
    """`PostgresResource` that connects to a local DuckDB file in demo mode.

    Every field below either has a demo-safe default or is only read when
    `demo_mode` is false, so this resource never requires a manual env var
    or credential to run the demo.
    """

    host: str = Field(default="localhost", description="PostgreSQL host (unused in demo mode)")
    database: str = Field(default="iso_ne_demo", description="Database name (unused in demo mode)")
    username: str = Field(default="demo_user", description="Database username (unused in demo mode)")
    password: str = Field(default="", description="Database password (unused in demo mode)")

    demo_mode: bool = Field(
        default=True,
        description=(
            "Serve a local DuckDB connection instead of a real Postgres "
            "connection. Set false and supply real Postgres credentials to "
            "write to ISO-NE's actual landing zone -- no other code changes "
            "required."
        ),
    )

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        if not self.demo_mode:
            conn = super().get_connection()
            try:
                yield conn
            finally:
                conn.close()
            return

        conn = connect_with_retry(demo_duckdb_path())
        try:
            yield conn
        finally:
            conn.close()


class DemoOracleResource(OracleResource):
    """`OracleResource` that is simply never connected to in demo mode.

    `legacy_oracle_extract` calls this resource's `get_connection()` only
    when `demo_mode: false` -- demo mode reads synthetic data instead and
    never touches this resource at all. Every field has a demo-safe default
    so the component can still be constructed with zero setup.
    """

    host: str = Field(default="localhost", description="Oracle DB host (unused in demo mode)")
    service_name: str = Field(default="FREEPDB1", description="Oracle service name (unused in demo mode)")
    username: str = Field(default="demo_user", description="Database username (unused in demo mode)")
    password: str = Field(default="", description="Database password (unused in demo mode)")
