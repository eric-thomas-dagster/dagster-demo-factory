"""Demo-mode seam on top of the REAL registry Postgres IO manager.

`PostgresIOManagerComponent` (imported below, unmodified) is the genuine
community-registry component installed via
`dagster-component add postgres_io_manager` -- it stores Dagster assets as
Postgres tables via SQLAlchemy + psycopg2
(`src/noleggiare/components/postgres_io_manager/`). This module does not
reimplement it; it adds exactly the seam `templates/demo_mode_pattern.py`
prescribes for a component whose extension point is a *resource* rather
than a single method: `demo_mode` swaps the ENTIRE IO manager resource this
component registers for `dagster_duckdb_pandas.DuckDBPandasIOManager` -- a
different real, published, native Dagster integration (`dagster-duckdb`),
not a home-made stand-in -- because no live Postgres account exists for
this prospect.

Every field the real component declares (`host`, `port`, `database`,
`user`, `password_env_var`, `default_schema`, `if_exists`,
`partition_column`) stays on this subclass completely unchanged, so the
YAML schema is identical in both modes per house rules. `demo_mode: false`
calls `super().build_defs()` -- the exact, untouched registry component --
so pointing this at a live Postgres instance is a one-line change plus
real credentials, never a rewrite.

Assets stay badged `kinds={"postgres"}` on their own `AssetSpec`s
regardless of which engine backs them (`shared_finance_warehouse`,
`noleggiare_rental_ops`, `tomasi_dealer_ops` defs.yaml) -- the badge is the
visual-fidelity story; this component is what makes the badge honest by
actually performing I/O through a real IO manager rather than a no-op body.
"""

from pathlib import Path

import dagster as dg
from pydantic import Field

from noleggiare.components.postgres_io_manager import PostgresIOManagerComponent

DEMO_DUCKDB_PATH = Path(__file__).parent.parent / "demo_data" / "noleggiare_postgres.duckdb"


class DemoPostgresIOManagerComponent(PostgresIOManagerComponent):
    """`PostgresIOManagerComponent` that writes to local DuckDB in demo mode.

    Set `demo_mode: false` and supply real `host` / `database` / `user` /
    `password_env_var` to run this exact component against a live
    PostgreSQL instance -- nothing else in the graph changes.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Write to a local DuckDB file instead of a live PostgreSQL "
            "database -- no PostgreSQL credentials exist for this prospect. "
            "Set false and supply host/database/user/password_env_var "
            "(the real PostgresIOManagerComponent's own fields, unchanged) "
            "to run against a real Postgres instance."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            return super().build_defs(context)

        from dagster_duckdb_pandas import DuckDBPandasIOManager

        DEMO_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        io_manager = DuckDBPandasIOManager(
            database=str(DEMO_DUCKDB_PATH),
            schema=self.default_schema,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
