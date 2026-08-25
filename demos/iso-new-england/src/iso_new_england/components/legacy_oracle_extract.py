"""Custom component: legacy Oracle interval-telemetry extract.

No native or community-registry component covers "extract + land arbitrary
Oracle telemetry with a fake network boundary for demo mode" -- the closest
registry fits were checked and ruled out:

- `database_replication` (Sling-backed): no `partitions_def` support, and its
  I/O seam is `SlingResource.replicate()` against a real connection string --
  not fakeable without a running source and target database.
- `rest_api_fetcher` / `odata_ingestion`: still require a live endpoint at
  materialize time, same problem.

So this is a rung-4 custom component per the escalation ladder, built on the
rung-3 `oracle_resource` registry component (subclassed in `resources.py`)
for the real-mode connection. See LEARNINGS.md for the recorded gap.

This is today's actual pattern at ISO-NE (a fixed daily Oracle batch), kept
on a schedule rather than a sensor -- the deliberate contrast with
`external_feed_raw`'s sensor-driven trigger is the demo's opening beat.
"""

import dagster as dg
from pydantic import Field

from iso_new_england.components.partitions import DAILY_PARTITIONS_DEF
from iso_new_england.components.resources import DemoOracleResource, DemoPostgresResource
from iso_new_england.demo_data.generators import generate_readings_frame
from iso_new_england.demo_data.warehouse import upsert_partition

_READINGS_RAW_COLUMNS = {
    "reporting_point_id": "VARCHAR",
    "event_date": "VARCHAR",
    "interval_ending": "VARCHAR",
    "reading_mw": "DOUBLE",
    "quality_flag": "VARCHAR",
    "source_system": "VARCHAR",
}


class LegacyOracleExtractComponent(dg.Component, dg.Resolvable, dg.Model):
    """Pulls one day's interval-telemetry batch from the legacy Oracle system.

    Real mode: queries Oracle via `oracle` and lands rows in the Postgres
    landing zone via `landing`. Demo mode: generates deterministic synthetic
    readings instead, and never touches either resource's connection.
    """

    oracle: DemoOracleResource
    landing: DemoPostgresResource
    demo_mode: bool = Field(default=True, description="Serve synthetic readings instead of querying Oracle.")
    demo_seed: int = Field(default=20260826, description="Seed for deterministic synthetic generation.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=dg.AssetKey(["raw", "legacy_oracle_extract"]),
            description=(
                "Daily interval-telemetry batch extracted from the legacy Oracle "
                "system, ahead of the Postgres migration. Runs on a fixed schedule "
                "today -- see `legacy_oracle_schedule` -- which is exactly the "
                "'dumb schedule regardless of whether work is needed' pain the "
                "brief names."
            ),
            group_name="ingestion",
            kinds={"oracle"},
            partitions_def=DAILY_PARTITIONS_DEF,
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(specs=[spec])
        def legacy_oracle_extract(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            event_date = context.partition_key
            frame = self._extract(context, event_date)

            with self.landing.get_connection() as conn:
                upsert_partition(
                    conn,
                    schema="raw",
                    table="legacy_oracle_extract",
                    df=frame,
                    match={"event_date": event_date},
                    ddl_columns=_READINGS_RAW_COLUMNS,
                )

            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": self.demo_mode,
                    "event_date": event_date,
                }
            )

        return dg.Definitions(assets=[legacy_oracle_extract])

    def _extract(self, context: dg.AssetExecutionContext, event_date: str):
        """The network boundary. Real mode queries Oracle; demo mode fakes it."""
        if not self.demo_mode:
            with self.oracle.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "select reporting_point_id, event_date, interval_ending, "
                    "reading_mw, quality_flag, source_system "
                    "from telemetry.interval_readings where event_date = :event_date",
                    event_date=event_date,
                )
                columns = [c[0].lower() for c in cursor.description]
                import pandas as pd

                return pd.DataFrame(cursor.fetchall(), columns=columns)

        return generate_readings_frame(event_date, self.demo_seed)
