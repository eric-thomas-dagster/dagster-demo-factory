"""Custom component: external operations-advisory feed ingestion.

Same registry gap as `legacy_oracle_extract.py` -- no ingestion component in
the registry supports a partitioned, demo-mode-fakeable "poll a vendor feed"
pattern (searched "webhook", "rest api", "sling", "database_replication";
see LEARNINGS.md). Rung-4 custom component.

This is the asset `external_feed_arrival_sensor` (`defs/sensors/`) triggers:
event-driven, not on a fixed schedule -- the direct answer to "trigger work
only when needed" from the brief. Whether a day's advisory batch has
"arrived" is real mock-source state in `demo_data/feed_state.py`, not a demo
toggle -- a day with nothing published yet genuinely has zero rows, exactly
as it would against the real vendor feed.
"""

import dagster as dg
from pydantic import Field

from iso_new_england.components.partitions import DAILY_PARTITIONS_DEF
from iso_new_england.components.resources import DemoPostgresResource
from iso_new_england.demo_data.feed_state import advisory_has_arrived
from iso_new_england.demo_data.generators import generate_advisory_frame
from iso_new_england.demo_data.warehouse import upsert_partition

_ADVISORY_RAW_COLUMNS = {
    "notice_id": "VARCHAR",
    "event_date": "VARCHAR",
    "issued_at": "VARCHAR",
    "category": "VARCHAR",
    "severity": "VARCHAR",
    "region": "VARCHAR",
    "message_summary": "VARCHAR",
}


class ExternalFeedComponent(dg.Component, dg.Resolvable, dg.Model):
    """Pulls one day's operations-advisory batch from the external feed, if published.

    Real mode: polls the vendor's advisory API. Demo mode: reads the mock
    feed-arrival state and generates deterministic synthetic advisories for
    days the vendor has "published."
    """

    landing: DemoPostgresResource
    demo_mode: bool = Field(default=True, description="Serve synthetic advisories instead of polling the vendor feed.")
    demo_seed: int = Field(default=20260826, description="Seed for deterministic synthetic generation.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=dg.AssetKey(["raw", "external_feed_raw"]),
            description=(
                "Operations-advisory batch from the external vendor feed, for "
                "days the vendor has published one. `external_feed_arrival_sensor` "
                "triggers this the moment a new day lands, replacing the fixed "
                "hourly/3-hourly schedule the legacy Oracle batch still runs on."
            ),
            group_name="ingestion",
            kinds={"oracle"},
            partitions_def=DAILY_PARTITIONS_DEF,
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(specs=[spec])
        def external_feed_raw(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            event_date = context.partition_key
            frame = self._poll(context, event_date)

            with self.landing.get_connection() as conn:
                upsert_partition(
                    conn,
                    schema="raw",
                    table="external_feed_raw",
                    df=frame,
                    match={"event_date": event_date},
                    ddl_columns=_ADVISORY_RAW_COLUMNS,
                )

            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": self.demo_mode,
                    "event_date": event_date,
                }
            )

        return dg.Definitions(assets=[external_feed_raw])

    def _poll(self, context: dg.AssetExecutionContext, event_date: str):
        """The network boundary. Real mode polls the vendor API; demo mode fakes it."""
        if not self.demo_mode:
            raise NotImplementedError(
                "Real-mode advisory-feed polling is not implemented in this demo. "
                "Replace this branch with the real vendor client when connecting "
                "to ISO-NE's actual operations-advisory feed."
            )

        if not advisory_has_arrived(event_date):
            context.log.info(
                "Mock advisory feed has nothing published for %s yet. Run "
                "`python -m iso_new_england.demo_data.simulate_new_advisory %s` "
                "to simulate it landing, then let the sensor pick it up.",
                event_date,
                event_date,
            )
            return generate_advisory_frame(event_date, self.demo_seed).iloc[0:0]

        return generate_advisory_frame(event_date, self.demo_seed)
