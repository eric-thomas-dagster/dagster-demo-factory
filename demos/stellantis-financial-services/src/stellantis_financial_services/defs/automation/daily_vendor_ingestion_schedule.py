"""The one fixed-time trigger in this demo: the daily vendor file cutoff.

Vendor files (dealer originations, servicer payments, credit bureau,
floorplan advances) land overnight; this schedule is the real deadline SFS
runs against today with its ~700 SSIS packages -- everything downstream of
bronze recomputes automatically via `AutomationCondition.eager()`
(`defs/transformation/*/defs.yaml`), so this is the only place a fixed time
appears. That contrast -- one scheduled trigger at the root, declarative
automation cascading the rest -- is the direct answer to "the solution is
not as scalable or flexible as they want."
"""

import dagster as dg

daily_vendor_ingestion_job = dg.define_asset_job(
    name="daily_vendor_ingestion_job",
    description="Pulls the day's batch from every vendor feed (bronze).",
    selection=dg.AssetSelection.groups("bronze"),
)

daily_vendor_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    daily_vendor_ingestion_job,
    hour_of_day=5,
    minute_of_hour=0,
    name="daily_vendor_ingestion_schedule",
    description=(
        "Runs at 5am ET, once overnight vendor file drops are expected to have "
        "landed -- the fixed cadence every downstream mart's eager automation "
        "condition reacts to."
    ),
)
