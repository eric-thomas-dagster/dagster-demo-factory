"""Custom component: shipment event ingestion.

Stands in for Northwind's homegrown TMS shipment-event feed -- an internal
system, not a vendor with a Dagster integration, so this is a rung-4 custom
component like `carrier_rate_feed.py`. See that module's docstring for the
registry searches that ruled out rungs 1-3.
"""

import dagster as dg
from pydantic import Field

from northwind_logistics.components.resources import DemoWarehouseResource
from northwind_logistics.demo_data.generators import generate_shipment_events_frame
from northwind_logistics.demo_data.warehouse import upsert_partition

SHIPMENT_PARTITIONS_DEF = dg.DailyPartitionsDefinition(start_date="2026-08-15", end_date="2026-08-25", timezone="America/New_York")


class ShipmentEventsComponent(dg.Component, dg.Resolvable, dg.Model):
    """Pulls one day's shipment events from the TMS.

    Real mode: reads the TMS event stream and lands rows in Snowflake.
    Demo mode: generates deterministic synthetic shipment events instead.
    """

    warehouse: DemoWarehouseResource
    demo_mode: bool = Field(default=True, description="Serve synthetic shipment events instead of reading the TMS.")
    demo_seed: int = Field(default=20260824, description="Seed for deterministic synthetic generation.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=dg.AssetKey(["raw", "shipment_events_raw"]),
            description="Shipment events from the TMS: one row per shipment, per day.",
            group_name="ingestion",
            kinds={"python"},
            partitions_def=SHIPMENT_PARTITIONS_DEF,
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(specs=[spec])
        def shipment_events_raw(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            event_date = context.partition_key
            frame = self._fetch_events(context, event_date=event_date)

            with self.warehouse.get_connection() as conn:
                upsert_partition(
                    conn,
                    schema="raw",
                    table="shipment_events_raw",
                    df=frame,
                    match={"event_date": event_date},
                )

            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": self.demo_mode,
                    "event_date": event_date,
                }
            )

        return dg.Definitions(assets=[shipment_events_raw])

    def _fetch_events(self, context: dg.AssetExecutionContext, event_date: str):
        """The network boundary. Real mode reads the TMS; demo mode fakes it."""
        if not self.demo_mode:
            raise NotImplementedError(
                "Real-mode TMS reads are not implemented in this demo. Replace "
                "this branch with the real TMS client when connecting to "
                "Northwind's actual shipment-event stream."
            )
        return generate_shipment_events_frame(event_date, self.demo_seed)
