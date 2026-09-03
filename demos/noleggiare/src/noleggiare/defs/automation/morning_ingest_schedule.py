"""The one schedule this demo needs. Raffaele's team sits in Finance in
Italy; the brief's only concrete number is the 07:00 CET Finance morning
read this build assumes for `fact_finance_consolidated_daily`'s freshness
policy (no SLA is confirmed in the AE notes -- see README). This schedule
targets 04:00 UTC (~05:00-06:00 CET/CEST), giving buffer before that read,
for the ingestion + per-company fact layer -- the eight assets on the plain
`DailyPartitionsDefinition`.

`fact_finance_consolidated_daily` (and everything downstream of it -- the
Snowflake-variant twin, the ML forecast, the Qlik export) is deliberately
NOT on this schedule. It's `date x company` multi-partitioned, which
`build_schedule_from_partitioned_job` doesn't target directly, and more
importantly it doesn't need to be: `AutomationCondition.eager()` on the
warehouse layer means it recomputes itself the moment its upstream facts
land -- "new source data flows through automatically, no cron job either
team has to remember exists," per the brief's own automation talk track.
The schedule handles the fixed morning ingest; declarative automation
handles everything that depends on it.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component: that component's partitioned-job mode can't
express a specific hour alongside a `partitions_def` (confirmed reading its
source in the detroit-dwsd build, reused finding in trafigura) -- the native
one-function call is simpler than a component for this.
"""

import dagster as dg

morning_ingest_job = dg.define_asset_job(
    name="noleggiare_morning_ingest_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["rental_bookings_raw"]),
        dg.AssetKey(["fleet_vehicles_raw"]),
        dg.AssetKey(["rental_contracts_raw"]),
        dg.AssetKey(["vehicle_inventory_raw"]),
        dg.AssetKey(["dealer_sales_raw"]),
        dg.AssetKey(["service_orders_raw"]),
        dg.AssetKey(["fact_rental_contract"]),
        dg.AssetKey(["fact_vehicle_sale"]),
    ),
)

morning_ingest_schedule = dg.build_schedule_from_partitioned_job(
    morning_ingest_job,
    hour_of_day=4,
    minute_of_hour=0,
    name="noleggiare_morning_ingest_schedule",
    description=(
        "Materializes the day's rental and dealer ingestion plus the "
        "per-company facts at 04:00 UTC (~05:00-06:00 CET/CEST depending on "
        "DST), ahead of the assumed 07:00 CET Finance morning read. The "
        "cross-company consolidated fact, residual-value forecast, and Qlik "
        "export recompute automatically once these land, via eager "
        "automation. Uses the DailyPartitionsDefinition's own UTC timezone -- "
        "build_schedule_from_partitioned_job rejects hour_of_day combined "
        "with an explicit execution_timezone, so the hour is expressed in "
        "the partitions_def's timezone rather than Europe/Rome directly."
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
