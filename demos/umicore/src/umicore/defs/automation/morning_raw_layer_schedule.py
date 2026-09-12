"""The one fixed-time trigger in this demo: the morning mesh-ingestion
cutoff.

No cutover time is confirmed in the brief -- 05:00 Europe/Brussels is this
build's own assumption of a reasonable overnight cadence for four
business units generating their own data products (see README), not a
stated SLA. This is deliberately the *only* schedule in the graph: the raw
layer is triggered on a real calendar deadline (each BU's own nightly batch
is expected to have landed by this hour), and everything downstream of it
-- staging, the corporate rollup, the executive extract, the carbon-
footprint report -- recomputes via eager automation conditions instead of
its own fixed window. That contrast (one genuine schedule at the true
data-arrival boundary, event-based propagation everywhere after it) is the
direct answer to the brief's "time-based, not event-based" pain.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component: that component's partitioned-job mode rejects
`cron_expression`/`execution_timezone` combined with
`partition_type`/`hour_of_day` -- a specific local hour can't be expressed
alongside a `partitions_def` (same registry gap recorded for
demos/rvu-tempcover, demos/partners-fcu). The native call is one function
call, not worth a component.
"""

import dagster as dg

morning_raw_layer_job = dg.define_asset_job(
    name="umicore_morning_raw_layer_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["battery_materials", "raw_cathode_production_batch"]),
        dg.AssetKey(["catalysis", "raw_ac_daily_feed"]),
        dg.AssetKey(["recycling", "raw_recycling_batch_yield"]),
        dg.AssetKey(["specialty_materials", "raw_specialty_materials_output"]),
    ),
)

morning_raw_layer_schedule = dg.build_schedule_from_partitioned_job(
    morning_raw_layer_job,
    hour_of_day=5,
    minute_of_hour=0,
    name="umicore_morning_raw_layer_schedule",
    description=(
        "Materializes the prior day's partition across all four business units' raw data "
        "products at 05:00 Europe/Brussels, once each BU's own overnight batch is expected "
        "to have landed. Everything downstream (staging, corporate rollup, executive "
        "extract, carbon-footprint report) recomputes via eager automation from here -- "
        "this is the only fixed-schedule trigger in the graph."
    ),
)
