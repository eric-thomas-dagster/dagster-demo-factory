"""Demo-mode seam on top of the REAL registry Snowflake IO manager.

`SnowflakeIOManagerComponent` (imported below, unmodified) is the genuine
community-registry component installed via
`dagster-component add snowflake_io_manager`
(`src/noleggiare/components/snowflake_io_manager/`) -- it registers the
real `dagster_snowflake_pandas.SnowflakePandasIOManager` from the native
`dagster-snowflake-pandas` package. This module does not reimplement it;
it adds the same resource-level demo_mode seam as
`demo_postgres_io_manager.py`: `demo_mode` swaps the entire IO manager
resource for `dagster_duckdb_pandas.DuckDBPandasIOManager` -- a real,
published, native Dagster integration, not a home-made stand-in -- because
no live Snowflake account exists for this prospect.

Every field the real component declares (`account`, `user`,
`password_env_var`, `database`, `warehouse`, `schema_name`, `role`) stays
unchanged on this subclass, so the YAML schema is identical in both modes.
`demo_mode: false` calls `super().build_defs()` -- the exact, untouched
registry component wrapping the real `SnowflakePandasIOManager` -- so this
is a one-line change plus real credentials, never a rewrite. This is the
literal "swap the warehouse, not the pipeline" story
`future_state_snowflake` exists to tell.

`fact_finance_consolidated_daily_snowflake` stays badged
`kinds={"snowflake"}` on its own `AssetSpec` regardless of which engine
backs it -- the badge is the visual-fidelity story; this component makes
the badge honest by performing genuine I/O through a real IO manager
rather than a no-op body.
"""

from pathlib import Path

import dagster as dg
from pydantic import Field

from noleggiare.components.snowflake_io_manager import SnowflakeIOManagerComponent

DEMO_DUCKDB_PATH = Path(__file__).parent.parent / "demo_data" / "noleggiare_snowflake.duckdb"


class DemoSnowflakeIOManagerComponent(SnowflakeIOManagerComponent):
    """`SnowflakeIOManagerComponent` that writes to local DuckDB in demo mode.

    Set `demo_mode: false` and supply real `account` / `user` /
    `password_env_var` / `database` / `warehouse` to run this exact
    component against a live Snowflake account -- nothing else in the
    graph changes.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Write to a local DuckDB file instead of a live Snowflake "
            "account -- no Snowflake credentials exist for this prospect. "
            "Set false and supply account/user/password_env_var/database/"
            "warehouse (the real SnowflakeIOManagerComponent's own fields, "
            "unchanged) to run against a real Snowflake account."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            return super().build_defs(context)

        from dagster_duckdb_pandas import DuckDBPandasIOManager

        DEMO_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        io_manager = DuckDBPandasIOManager(
            database=str(DEMO_DUCKDB_PATH),
            schema=self.schema_name or "public",
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
