"""Demo-mode subclass of `dagster_dbt.DbtProjectComponent`.

Two independent customizations, both demo-mode-only -- the real-mode path is
`super()`, untouched:

1. `get_asset_spec()` fixes the dependency edge from `stg_carrier_rates` to
   `raw/carrier_rate_raw`: the upstream is multi-partitioned (day x carrier)
   but `stg_carrier_rates` is only partitioned by day, so it needs a
   `MultiToSingleDimensionPartitionMapping` to depend on all four carrier
   partitions for its day rather than dagster's default same-key mapping
   (which would look for a literal day-only key in a multi-partitioned
   upstream and never find one).

2. `execute()` re-seeds the whole raw layer from the deterministic
   generators before every dbt build. This is a Serverless-specific
   workaround, not a stylistic choice: Dagster+ Serverless gives each run
   its own ephemeral disk (confirmed against Dagster's own docs), so the
   DuckDB file a *previous*, separate run wrote to is not guaranteed to
   still be there. Re-seeding is cheap (a handful of small deterministic
   generators) and makes every dbt run self-sufficient regardless of which
   container it lands on. It runs untracked (no `context=`) so it does not
   interfere with the Dagster-tracked, partition-scoped build that follows.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated, Any

import dagster as dg
import pandas as pd
from dagster_dbt import DbtCliResource, DbtProjectComponent

from northwind_logistics.demo_data.generators import (
    generate_carrier_rate_frame,
    generate_netsuite_gl_entries_frame,
    generate_salesforce_accounts_frame,
    generate_shipment_events_frame,
    generate_zendesk_tickets_frame,
)
from northwind_logistics.demo_data.state import (
    ANOMALY_CARRIER,
    ANOMALY_DATE,
    DEMO_WINDOW_END,
    DEMO_WINDOW_START,
    EXPECTED_CARRIERS,
    get_healed_partitions,
)
from northwind_logistics.demo_data.warehouse import connect_with_retry, demo_duckdb_path, write_table

_CARRIER_RATE_ASSET_KEY = dg.AssetKey(["raw", "carrier_rate_raw"])


@dataclass
class NorthwindDbtComponent(DbtProjectComponent):
    """`DbtProjectComponent` with a demo-mode raw-layer safety net and a partition-mapping fix."""

    demo_mode: Annotated[
        bool,
        dg.Resolver.default(description="Re-seed the raw layer from generators before every build."),
    ] = True

    def get_asset_spec(self, manifest: dict[str, Any], unique_id: str, project) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        if not unique_id.endswith(".stg_carrier_rates"):
            return base_spec
        return base_spec.replace_attributes(
            deps=[
                dg.AssetDep(
                    _CARRIER_RATE_ASSET_KEY,
                    partition_mapping=dg.MultiToSingleDimensionPartitionMapping(partition_dimension_name="date"),
                )
            ]
        )

    def execute(self, context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
        if self.demo_mode:
            _reseed_raw_layer(context.instance)
            # A fresh DbtCliResource, not the `dbt` passed in: dagster-dbt
            # prepares an isolated working copy of the project per `.cli()`
            # call, and reusing the same resource object for this untracked
            # pre-build and the tracked build below raced against that
            # preparation (surfaced as an intermittent "no dbt_project.yml
            # found" error in the second call).
            DbtCliResource(dbt.project_dir).cli(["build"], raise_on_error=False).wait()
        yield from super().execute(context, dbt)


def _reseed_raw_layer(instance: dg.DagsterInstance) -> None:
    healed = get_healed_partitions(instance)
    demo_seed = 20260824
    dates = pd.date_range(DEMO_WINDOW_START, DEMO_WINDOW_END, freq="D").strftime("%Y-%m-%d")

    carrier_frames = []
    for event_date in dates:
        for carrier in EXPECTED_CARRIERS:
            is_anomaly = event_date == ANOMALY_DATE and carrier == ANOMALY_CARRIER
            if is_anomaly and f"{carrier}|{event_date}" not in healed:
                continue  # the rate feed "never arrived" for this carrier/day
            carrier_frames.append(generate_carrier_rate_frame(event_date, carrier, demo_seed))
    carrier_rate_df = pd.concat(carrier_frames, ignore_index=True)

    shipment_df = pd.concat(
        (generate_shipment_events_frame(event_date, demo_seed) for event_date in dates),
        ignore_index=True,
    )

    conn = connect_with_retry(demo_duckdb_path())
    try:
        write_table(conn, schema="raw", table="carrier_rate_raw", df=carrier_rate_df)
        write_table(conn, schema="raw", table="shipment_events_raw", df=shipment_df)
        write_table(conn, schema="raw", table="salesforce_accounts", df=generate_salesforce_accounts_frame(demo_seed))
        write_table(
            conn,
            schema="raw",
            table="zendesk_tickets",
            df=generate_zendesk_tickets_frame(demo_seed, DEMO_WINDOW_START, DEMO_WINDOW_END),
        )
        write_table(
            conn,
            schema="raw",
            table="netsuite_gl_entries",
            df=generate_netsuite_gl_entries_frame(demo_seed, DEMO_WINDOW_START, DEMO_WINDOW_END),
        )
    finally:
        conn.close()
