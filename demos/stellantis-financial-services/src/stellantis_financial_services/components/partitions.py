"""Partition dimensions shared by the whole vendor-file -> Fabric pipeline flow.

`DEALER_GROUPS` are the four regional dealer-group feeds the brief calls out
-- dealer floorplan financing arrives per-region, which is a genuine second
domain axis (not a fabricated one), so `MultiPartitionsDefinition` is
warranted for `raw_dealer_floorplan_feed` specifically. Every other asset is
date-only: a `MultiPartitionsDefinition` on assets with no real second
dimension would just be decoration.

Open-ended (no `end_date`) so both always extend through "today".
"""

import dagster as dg

DEALER_GROUPS = ["midwest", "northeast", "south", "west"]

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-07-01",
    timezone="America/Detroit",
)

DEALER_GROUP_PARTITIONS_DEF = dg.StaticPartitionsDefinition(DEALER_GROUPS)

DATE_DEALER_GROUP_PARTITIONS_DEF = dg.MultiPartitionsDefinition(
    {"date": DAILY_PARTITIONS_DEF, "dealer_group": DEALER_GROUP_PARTITIONS_DEF}
)
