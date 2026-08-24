"""Custom component: carrier rate feed ingestion.

No native or community-registry component covers arbitrary carrier-rate REST
APIs (searched "rest api", "http polling", "rate limit", "carrier", "freight",
"webhook source", "generic api" against the community registry -- nothing
fit; `http_poll_sensor` exists but builds a sensor, not a partitioned asset).
This is therefore a rung-4 custom component, per the escalation ladder in
CLAUDE.md.

Demo mode fakes the outermost network call only (`_fetch_rates`, standing in
for the real FedEx/UPS/regional-LTL API pull) and writes through the same
`DemoWarehouseResource` a real deployment would use for Snowflake -- see
`components/resources.py`.
"""

import dagster as dg
from pydantic import Field

from northwind_logistics.components.resources import DemoWarehouseResource
from northwind_logistics.demo_data.generators import generate_carrier_rate_frame
from northwind_logistics.demo_data.state import (
    ANOMALY_CARRIER,
    ANOMALY_DATE,
    EXPECTED_CARRIERS,
    get_healed_partitions,
)
from northwind_logistics.demo_data.warehouse import upsert_partition

_CARRIER_RATE_RAW_COLUMNS = {
    "carrier": "VARCHAR",
    "event_date": "VARCHAR",
    "lane": "VARCHAR",
    "rate_per_mile": "DOUBLE",
    "fuel_surcharge_pct": "DOUBLE",
    "quoted_at": "VARCHAR",
}

CARRIER_PARTITIONS_DEF = dg.MultiPartitionsDefinition(
    {
        "date": dg.DailyPartitionsDefinition(start_date="2026-08-15", end_date="2026-08-25", timezone="America/New_York"),
        "carrier": dg.StaticPartitionsDefinition(EXPECTED_CARRIERS),
    }
)


class CarrierRateFeedComponent(dg.Component, dg.Resolvable, dg.Model):
    """Pulls one carrier's freight rate quotes for one day.

    Real mode: hits the carrier's REST API and lands rows in Snowflake.
    Demo mode: generates deterministic synthetic rates instead, and can
    simulate a carrier's data simply not arriving for a given day (the
    late-carrier-data pain point from the brief).
    """

    warehouse: DemoWarehouseResource
    demo_mode: bool = Field(default=True, description="Serve synthetic rates instead of calling carrier APIs.")
    demo_seed: int = Field(default=20260824, description="Seed for deterministic synthetic generation.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=dg.AssetKey(["raw", "carrier_rate_raw"]),
            description=(
                "Freight rate quotes pulled from carrier rate APIs (FedEx, UPS, "
                "two regional LTL carriers), one partition per carrier per day."
            ),
            group_name="ingestion",
            kinds={"python", "api"},
            partitions_def=CARRIER_PARTITIONS_DEF,
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(specs=[spec])
        def carrier_rate_raw(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            multi_key = context.partition_key
            event_date = multi_key.keys_by_dimension["date"]
            carrier = multi_key.keys_by_dimension["carrier"]

            frame = self._fetch_rates(context, event_date=event_date, carrier=carrier)

            with self.warehouse.get_connection() as conn:
                upsert_partition(
                    conn,
                    schema="raw",
                    table="carrier_rate_raw",
                    df=frame,
                    match={"event_date": event_date, "carrier": carrier},
                    ddl_columns=_CARRIER_RATE_RAW_COLUMNS,
                )

            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": self.demo_mode,
                    "carrier": carrier,
                    "event_date": event_date,
                }
            )

        return dg.Definitions(assets=[carrier_rate_raw])

    def _fetch_rates(self, context: dg.AssetExecutionContext, event_date: str, carrier: str):
        """The network boundary. Real mode calls the carrier's API; demo mode fakes it."""
        if not self.demo_mode:
            raise NotImplementedError(
                "Real-mode carrier API calls are not implemented in this demo. "
                "Replace this branch with the real FedEx/UPS/regional-LTL client "
                "when connecting to Northwind's actual carrier accounts."
            )

        healed = get_healed_partitions(context.instance)
        # str(MultiPartitionKey) is the canonical form healed_partitions
        # entries are compared against -- matches ANOMALY_PARTITION_KEY.
        partition_key = str(context.partition_key)
        is_the_planted_anomaly = event_date == ANOMALY_DATE and carrier == ANOMALY_CARRIER

        if is_the_planted_anomaly and partition_key not in healed:
            context.log.warning(
                "Demo mode: simulating a missed pickup window for %s on %s -- "
                "no rate data arrived for this partition.",
                carrier,
                event_date,
            )
            return generate_carrier_rate_frame(event_date, carrier, self.demo_seed).iloc[0:0]

        return generate_carrier_rate_frame(event_date, carrier, self.demo_seed)
