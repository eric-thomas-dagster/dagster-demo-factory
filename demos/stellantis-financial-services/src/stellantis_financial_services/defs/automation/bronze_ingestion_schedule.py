"""Fixed daily schedule for the bronze ingestion layer.

Vendor files land overnight; SFS's real deadline is having the day's batch
ready before the desk opens. Triggers each bronze Fabric pipeline at 6am ET
so a genuinely late vendor drop shows up as a stale/blocked asset well before
business hours -- silver and gold pick it up automatically via
`AutomationCondition.eager()` once bronze lands.
"""

import dagster as dg

# raw_dealer_floorplan_feed carries its own (date, dealer_group) partitioning
# and is scheduled separately below -- a job's assets must share one
# partitions_def for `build_schedule_from_partitioned_job` to derive run
# requests from it.
_DATE_ONLY_BRONZE_ASSET_KEYS = [
    dg.AssetKey(["raw", "raw_loan_originations"]),
    dg.AssetKey(["raw", "raw_lease_originations"]),
    dg.AssetKey(["raw", "raw_payment_transactions"]),
    dg.AssetKey(["raw", "raw_credit_bureau_pull"]),
]

bronze_ingestion_job = dg.define_asset_job(
    name="bronze_ingestion_job",
    selection=dg.AssetSelection.assets(*_DATE_ONLY_BRONZE_ASSET_KEYS),
)

bronze_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    bronze_ingestion_job,
    hour_of_day=6,
    minute_of_hour=0,
    name="bronze_ingestion_schedule",
    description=(
        "Triggers each single-vendor-feed Fabric pipeline at 6am (America/Detroit -- the "
        "partitions_def's timezone), ahead of the trading desk's open."
    ),
)

dealer_floorplan_ingestion_job = dg.define_asset_job(
    name="dealer_floorplan_ingestion_job",
    selection=dg.AssetSelection.assets(dg.AssetKey(["raw", "raw_dealer_floorplan_feed"])),
)

dealer_floorplan_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    dealer_floorplan_ingestion_job,
    hour_of_day=6,
    minute_of_hour=30,
    name="dealer_floorplan_ingestion_schedule",
    description=(
        "Triggers the per-dealer-group floorplan Fabric pipelines at 6:30am ET, 30 minutes "
        "behind the single-vendor feeds since dealer SFTP drops land slightly later."
    ),
)
