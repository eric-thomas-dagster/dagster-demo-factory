"""Partition dimensions for the SFS Fabric pipeline demo.

`dealer_group` is a genuine second domain axis -- dealer floorplan feeds
arrive per regional dealer group -- so `MultiPartitionsDefinition` is
warranted only on `raw_dealer_floorplan_feed`, per CLAUDE.md's partition
guidance. Every other asset carries the plain daily partition.

`DAILY_PARTITIONS_DEF`'s start date and (default UTC) timezone must stay
equal to the `partition_start`/timezone the `cron_schedule` component
instance builds internally (`defs/automation/defs.yaml`) --
`define_asset_job` errors if a job's explicit `partitions_def` doesn't
equal the selected assets' own `partitions_def`.
"""

import dagster as dg

DEALER_GROUPS = ["midwest", "northeast", "south", "west"]

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(start_date="2026-08-01")

DEALER_GROUP_PARTITIONS_DEF = dg.StaticPartitionsDefinition(DEALER_GROUPS)

DATE_DEALER_GROUP_PARTITIONS_DEF = dg.MultiPartitionsDefinition(
    {"date": DAILY_PARTITIONS_DEF, "dealer_group": DEALER_GROUP_PARTITIONS_DEF}
)
