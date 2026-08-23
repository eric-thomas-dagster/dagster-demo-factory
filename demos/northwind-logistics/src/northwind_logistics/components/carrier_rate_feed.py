"""Carrier rate feed ingestion. Read templates/demo_mode_pattern.py first.

No native Dagster integration or community-registry component covers
arbitrary carrier rate APIs (FedEx / UPS / regional LTL) -- confirmed via
`dagster-component search "rest api"` / `search "api" --category ingestion`
before writing this. This component is therefore the "real" implementation
as well as the demo one: `demo_mode=False` is the seam where a future build
would add the actual per-carrier HTTP polling.

Real carrier API integrations are explicitly out of scope for this build
(see the brief's "Explicitly out of scope" section) -- `demo_mode=False`
raises rather than pretending to call carriers that were never wired up.
"""

import dagster as dg
from pydantic import Field

from northwind_logistics.components.snowflake_demo import DemoSnowflakeResource
from northwind_logistics.components.warehouse_io import replace_partition_rows
from northwind_logistics.demo_data.generators import generate_carrier_rate_rows


class CarrierRateFeedComponent(dg.Component, dg.Resolvable, dg.Model):
    """Daily freight rate quotes from FedEx / UPS / two regional LTL carriers.

    Falls back to daily-only time partitioning with carrier as a column
    (see the brief) rather than a `MultiPartitionsDefinition` over
    date x carrier -- that mapping would need a custom partition mapping
    between this asset and the daily-partitioned dbt models downstream,
    which was judged too costly for this build window. This weakens the
    recovery money-shot slightly: a heal + rematerialize recovers one
    carrier's data for a day, but the granularity of a Dagster run is the
    whole day's feed, not one carrier within it.
    """

    demo_mode: bool = Field(default=True, description="Serve deterministic synthetic rows.")
    demo_seed: int = Field(default=20260824, description="Base seed for synthetic generation.")
    start_date: str = Field(default="2026-08-17", description="First partition date (YYYY-MM-DD).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = dg.DailyPartitionsDefinition(start_date=self.start_date)
        spec = dg.AssetSpec(
            key="carrier_rate_raw",
            description=(
                "Daily freight rate quotes for every carrier x lane. Stands in for the "
                "FedEx / UPS / regional-LTL rate APIs. Two carriers land late ~15% of the "
                "time in production; `regional_ltl_b` is deliberately missing for "
                "2026-08-21 until healed, so `carrier_rate_arrival` has something to catch."
            ),
            group_name="ingestion",
            kinds={"python"},
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(specs=[spec], partitions_def=partitions_def, name="carrier_rate_raw")
        def _carrier_rate_raw(
            context: dg.AssetExecutionContext, warehouse: DemoSnowflakeResource
        ) -> dg.MaterializeResult:
            rate_date = context.partition_key
            if not self.demo_mode:
                raise NotImplementedError(
                    "Real FedEx/UPS/regional-LTL rate API integration is out of scope for "
                    "this demo build. Set demo_mode: true, or implement a real fetch here."
                )
            frame = generate_carrier_rate_rows(rate_date, self.demo_seed)
            with warehouse.get_connection() as conn:
                replace_partition_rows(
                    conn,
                    schema="raw",
                    table="carrier_rate_raw",
                    frame=frame,
                    partition_column="rate_date",
                    partition_value=rate_date,
                )
            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": True,
                    "source": dg.MetadataValue.text(
                        "synthetic -- set demo_mode: false to poll real carrier rate APIs"
                    ),
                }
            )

        return dg.Definitions(assets=[_carrier_rate_raw])
