"""Shipment event stream ingestion. Read templates/demo_mode_pattern.py first.

Northwind's shipment events come from their own internal event pipeline, not
a third-party vendor -- there is no registry component or native Dagster
integration to subclass. Real ingestion here is out of scope for this build
(see the brief's "Explicitly out of scope" section); `demo_mode=False` raises
rather than pretending to read from a real event store that was never wired.
"""

import dagster as dg
from pydantic import Field

from northwind_logistics.components.snowflake_demo import DemoSnowflakeResource
from northwind_logistics.components.warehouse_io import replace_partition_rows
from northwind_logistics.demo_data.generators import generate_shipment_events_rows


class ShipmentEventsFeedComponent(dg.Component, dg.Resolvable, dg.Model):
    """Daily shipment scan events (~4M rows/day in production)."""

    demo_mode: bool = Field(default=True, description="Serve deterministic synthetic rows.")
    demo_seed: int = Field(default=20260824, description="Base seed for synthetic generation.")
    demo_row_count: int = Field(
        default=2_500,
        description="Rows per partition, scaled down from the real ~4M/day volume for demo runtime.",
    )
    start_date: str = Field(default="2026-08-17", description="First partition date (YYYY-MM-DD).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = dg.DailyPartitionsDefinition(start_date=self.start_date)
        spec = dg.AssetSpec(
            key="shipment_events_raw",
            description=(
                "Daily shipment scan events. Stands in for Northwind's internal event "
                "pipeline (~4M rows/day in production; scaled down here for demo runtime). "
                "A couple of partitions carry a 3x volume bump to gesture at Oct-Dec peak season."
            ),
            group_name="ingestion",
            kinds={"python"},
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(specs=[spec], partitions_def=partitions_def, name="shipment_events_raw")
        def _shipment_events_raw(
            context: dg.AssetExecutionContext, warehouse: DemoSnowflakeResource
        ) -> dg.MaterializeResult:
            event_date = context.partition_key
            if not self.demo_mode:
                raise NotImplementedError(
                    "Real shipment event ingestion is out of scope for this demo build. "
                    "Set demo_mode: true, or implement a real fetch here."
                )
            frame = generate_shipment_events_rows(event_date, self.demo_seed, self.demo_row_count)
            with warehouse.get_connection() as conn:
                replace_partition_rows(
                    conn,
                    schema="raw",
                    table="shipment_events_raw",
                    frame=frame,
                    partition_column="event_date",
                    partition_value=event_date,
                )
            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": True,
                    "source": dg.MetadataValue.text(
                        "synthetic -- set demo_mode: false to read the real event pipeline"
                    ),
                }
            )

        return dg.Definitions(assets=[_shipment_events_raw])
