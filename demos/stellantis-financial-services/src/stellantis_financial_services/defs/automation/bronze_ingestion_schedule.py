"""Daily bronze-ingestion schedule -- the real deadline the floorplan
lateness check (see `defs/checks/raw_dealer_floorplan_feed_lateness.py`)
holds every region to. Bronze feeds are triggered on this cron rather than
via `AutomationCondition.eager()` -- there is no upstream Dagster asset to
react to; a vendor file landing is the real-world trigger, and a schedule is
the honest way to represent "we expect this by a fixed time daily."

Scoped to the three Fabric-migrated, single (`date`-partitioned) bronze
feeds Dagster actually triggers. `raw_dealer_floorplan_feed` and
`raw_credit_bureau_pull` are deliberately not here -- they're still legacy
SSIS packages under SFS's own scheduler (see
`defs/legacy_assets/legacy_assets.py`), so Dagster has nothing to trigger for
them; the dealer floorplan feed's second `dealer_group` dimension is
exercised live in the demo instead (see DEMO_SCRIPT.md's money shot).
"""

import dagster as dg

BRONZE_DAILY_ASSET_KEYS = [
    dg.AssetKey(["raw_loan_originations"]),
    dg.AssetKey(["raw_lease_originations"]),
    dg.AssetKey(["raw_payment_transactions"]),
]

bronze_ingestion_job = dg.define_asset_job(
    name="bronze_ingestion_job",
    selection=dg.AssetSelection.assets(*BRONZE_DAILY_ASSET_KEYS),
    description="Triggers the day's Fabric-migrated vendor-file bronze feeds (loan/lease originations, payments).",
)

bronze_ingestion_daily_schedule = dg.build_schedule_from_partitioned_job(
    bronze_ingestion_job,
    name="bronze_ingestion_daily_6am",
    hour_of_day=6,
    minute_of_hour=0,
    description=(
        "Fires daily at 6am -- the overnight-batch cutoff the dealer floorplan lateness check holds "
        "every region to. Everything downstream (staging, marts, reporting) recomputes itself via "
        "AutomationCondition.eager() once bronze lands; nothing downstream needs its own schedule."
    ),
)
