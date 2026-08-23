"""Shared warehouse resource for every ingestion asset and asset check.

Real dbt runs manage their own Snowflake/DuckDB connection independently
(see dbt_project/profiles.yml) -- this resource is for the Dagster-native
raw writes and the checks that read back from the warehouse to verify them.
"""

import os

import dagster as dg

from northwind_logistics.components.snowflake_demo import DemoSnowflakeResource


def build_warehouse_resource() -> DemoSnowflakeResource:
    """Construct the warehouse resource from the environment.

    Shared between the project's `Definitions` (regular resource injection)
    and `DemoFivetranAccountComponent.execute()`, which calls a Fivetran
    connector's sync directly rather than through a resource-injected asset
    parameter (see that component's docstring for why).
    """
    return DemoSnowflakeResource(
        demo_mode=os.environ.get("NORTHWIND_DEMO_MODE", "true").lower() != "false",
        demo_duckdb_path=os.environ.get("NORTHWIND_DUCKDB_PATH", "demo_data/warehouse.duckdb"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER", "demo-user"),
        # Never actually used to connect while demo_mode is true (get_connection
        # short-circuits to DuckDB) -- SnowflakeResource's own validation just
        # requires some auth method to be configured at construction time.
        password=os.environ.get("SNOWFLAKE_PASSWORD", "unused-in-demo-mode"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "NORTHWIND"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        schema_=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
    )


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"warehouse": build_warehouse_resource()})
