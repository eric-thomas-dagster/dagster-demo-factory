"""Shared partition definitions for assets that cross the legacy/Fabric
boundary. `raw_dealer_floorplan_feed` is declared in `defs/legacy_assets/` (a
plain external `AssetSpec`, no Fabric pipeline behind it) and is also the
`deps` target of `dim_dealer`, declared via `DemoFabricWorkspaceComponent` in
`defs/fabric_pipelines/defs.yaml`. Dagster requires the same asset key's
`PartitionsDefinition` to be structurally identical everywhere it's
referenced -- a single shared module is how that stays true without hand-
synchronizing two files.
"""

import dagster as dg

from stellantis_financial_services.demo_data.generators import DEALER_GROUPS

DAILY_PARTITIONS = dg.DailyPartitionsDefinition(start_date="2026-06-01")
DEALER_GROUP_PARTITIONS = dg.StaticPartitionsDefinition(DEALER_GROUPS)
DEALER_FEED_PARTITIONS = dg.MultiPartitionsDefinition(
    {"date": DAILY_PARTITIONS, "dealer_group": DEALER_GROUP_PARTITIONS}
)
DEALER_GROUP_ROLLUP_MAPPING = dg.MultiToSingleDimensionPartitionMapping(partition_dimension_name="dealer_group")
