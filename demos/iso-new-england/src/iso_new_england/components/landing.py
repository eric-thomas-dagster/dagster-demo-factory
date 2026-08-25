"""Custom component: copy one raw table into the Postgres landing zone.

No demo-mode branch here, unlike the ingestion components -- this step never
crosses a network boundary in either mode. It reads the just-extracted rows
for the partition out of `raw.<source_table>` and writes them into
`staged.<target_table>` via the same `DemoPostgresResource` (duckdb locally,
real Postgres in live mode). That single resource swap is the whole
migration story: Oracle stays Oracle-shaped in `raw`, and everything from
`staged` on is genuinely Postgres-shaped, in both modes.

Two instances of this one component (`defs/transformation/landing/`) produce
`staged_readings` and `staged_reference` -- no registry component covers an
opinionated "copy one partition between two schemas" step, and it is too
small and specific to justify a registry search of its own.
"""

import dagster as dg
from pydantic import Field

from iso_new_england.components.partitions import DAILY_PARTITIONS_DEF
from iso_new_england.components.resources import DemoPostgresResource
from iso_new_england.demo_data.warehouse import upsert_partition


class PostgresLandingComponent(dg.Component, dg.Resolvable, dg.Model):
    """Copies one day's rows from a `raw` table into a `staged` table, unchanged."""

    landing: DemoPostgresResource
    asset_name: str = Field(description="Output asset name, under the `staged` key prefix.")
    source_table: str = Field(description="Table name under the `raw` schema to read from.")
    upstream_asset_key: list[str] = Field(description="The raw asset this depends on, e.g. ['raw', 'legacy_oracle_extract'].")
    description: str = Field(default="")
    group_name: str = Field(default="landing")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=dg.AssetKey(["staged", self.asset_name]),
            deps=[dg.AssetKey(self.upstream_asset_key)],
            description=self.description,
            group_name=self.group_name,
            kinds={"postgres"},
            partitions_def=DAILY_PARTITIONS_DEF,
            automation_condition=dg.AutomationCondition.eager(),
        )

        @dg.multi_asset(specs=[spec], name=f"land_{self.asset_name}")
        def landing_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            event_date = context.partition_key

            with self.landing.get_connection() as conn:
                frame = conn.execute(
                    f"select * from raw.{self.source_table} where event_date = ?",
                    [event_date],
                ).fetch_df()
                upsert_partition(
                    conn,
                    schema="staged",
                    table=self.asset_name,
                    df=frame,
                    match={"event_date": event_date},
                )

            return dg.MaterializeResult(
                metadata={"dagster/row_count": len(frame), "event_date": event_date}
            )

        return dg.Definitions(assets=[landing_asset])
