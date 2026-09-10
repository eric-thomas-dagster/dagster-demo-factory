"""Demo-mode seam on top of the REAL registry SQL Server IO manager.

`MSSQLIOManagerComponent` (imported below, unmodified) is the genuine
community-registry component installed via
`dagster-component add mssql_io_manager` --
`src/partners_fcu/components/mssql_io_manager/` -- it writes Dagster assets
to a Microsoft SQL Server schema via SQLAlchemy + pymssql. This module does
not reimplement it; it adds exactly the resource-level seam
`templates/demo_mode_pattern.py` prescribes: `demo_mode` swaps the ENTIRE IO
manager resource this component registers for
`dagster_duckdb_pandas.DuckDBPandasIOManager` -- a real, published, native
Dagster integration (`dagster-duckdb`), not a home-made stand-in -- because
no live Partners FCU SQL Server credentials exist.

Every field the real component declares (`resource_key`,
`connection_url_env_var`, `schema_name`, `if_exists`) stays on this subclass
completely unchanged, so the YAML schema is identical in both modes.
`demo_mode: false` calls `super().build_defs()` -- the exact, untouched
registry component, hitting a real SQL Server instance via
`MSSQL_URL` -- so pointing this at Partners FCU's real SQL Server is a
one-line change plus a real connection string, never a rewrite.

The real `MSSQLIOManager` lands each partition in its own physical table
(`<asset>_<partition_key>`) -- a legitimate partitioning strategy for a raw
landing zone, but not one a dbt source can select from under one stable
name. `DuckDBPandasIOManager` (dagster-duckdb, also a real, native
integration) instead does column-scoped DELETE+INSERT per partition into one
table when given `partition_expr` metadata -- the scheme
`WarehouseTableAssetsComponent` already relies on. Swapping to it is a
demo-mode-only network-boundary substitution like any other in this file;
it is not a change to the real component's own behaviour, which is
untouched behind `demo_mode: false`.

Assets stay badged `kinds={"mssql"}` on their own `AssetSpec`s regardless of
which engine backs them (`defs/ingestion/defs.yaml`) -- the badge is the
visual-fidelity story; this component is what makes the badge honest by
performing genuine I/O through a real IO manager rather than a no-op body.
"""

import dagster as dg
from pydantic import Field

from partners_fcu.components.mssql_io_manager import MSSQLIOManagerComponent
from partners_fcu.demo_data.warehouse import demo_duckdb_path


class DemoMSSQLIOManagerComponent(MSSQLIOManagerComponent):
    """`MSSQLIOManagerComponent` that writes to local DuckDB in demo mode.

    Set `demo_mode: false` and supply a real `connection_url_env_var`
    (the real `MSSQLIOManagerComponent`'s own field, unchanged) to run this
    exact component against a live SQL Server instance -- nothing else in
    the graph changes.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Write to a local DuckDB file instead of a live SQL Server "
            "database -- no Partners FCU SQL Server credentials exist. "
            "Set false and supply connection_url_env_var (the real "
            "MSSQLIOManagerComponent's own field, unchanged) to run "
            "against Partners FCU's real SQL Server instance."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            return super().build_defs(context)

        from dagster_duckdb_pandas import DuckDBPandasIOManager

        io_manager = DuckDBPandasIOManager(
            database=demo_duckdb_path(),
            schema=self.schema_name,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
