"""Demo-mode resource swap for warehouse I/O.

Follows the "Variant: components whose seam is a resource rather than a
method" pattern from `templates/demo_mode_pattern.py`: subclass the real
`SnowflakeResource` unmodified, override only `get_connection()`. Every
ingestion component that writes to the warehouse takes this resource
unmodified -- flipping `demo_mode: false` and supplying real Snowflake
credentials is the entire migration to production.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dagster_snowflake import SnowflakeResource
from pydantic import Field

from northwind_logistics.demo_data.warehouse import connect_with_retry, demo_duckdb_path


class DemoWarehouseResource(SnowflakeResource):
    """`SnowflakeResource` that connects to a local DuckDB file in demo mode."""

    demo_mode: bool = Field(
        default=True,
        description=(
            "Serve a local DuckDB connection instead of a real Snowflake "
            "connection. Set false and supply real Snowflake credentials to "
            "write to a live account -- no other code changes required."
        ),
    )

    @contextmanager
    def get_connection(self, raw_conn: bool = True) -> Iterator[Any]:
        if not self.demo_mode:
            with super().get_connection(raw_conn=raw_conn) as conn:
                yield conn
            return

        conn = connect_with_retry(demo_duckdb_path())
        try:
            yield conn
        finally:
            conn.close()
