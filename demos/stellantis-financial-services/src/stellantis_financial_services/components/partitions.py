"""Shared partition definitions.

Daily to match the vendor file-drop cadence. `raw_dealer_floorplan_feed` (and
only that asset) also partitions by `dealer_group` -- floorplan advances
genuinely arrive as separate per-region batches from SFS's dealer network, so
a second dimension is real here in a way it wouldn't be for e.g. a single
daily loan-origination file. This is also the one asset with a planted
anomaly, so the second dimension is what makes "rematerialize just the one
late dealer group's one day" a genuine partition-scoped recovery story rather
than a whole-asset rerun.

`dim_dealer` depends on every `dealer_group` partition of
`raw_dealer_floorplan_feed` for a given date (it rolls up the full dealer
roster), so it maps the multi-partitioned upstream down to its own
date-only partitioning via `FLOORPLAN_TO_DATE_MAPPING`.
"""

import dagster as dg

DATE_PARTITIONS_DEF = dg.DailyPartitionsDefinition(start_date="2026-08-01", timezone="America/Detroit")

DEALER_GROUP_PARTITIONS_DEF = dg.StaticPartitionsDefinition(
    ["northeast_dealers", "midwest_dealers", "southeast_dealers", "west_dealers"]
)

FLOORPLAN_MULTI_PARTITIONS_DEF = dg.MultiPartitionsDefinition(
    {"date": DATE_PARTITIONS_DEF, "dealer_group": DEALER_GROUP_PARTITIONS_DEF}
)

FLOORPLAN_TO_DATE_MAPPING = dg.MultiToSingleDimensionPartitionMapping(partition_dimension_name="date")

# The demo's pre-seeded window -- independent of wall-clock "today" so
# `validate_e2e.py` and the shipped demo state are reproducible run to run.
DEMO_WINDOW_START = "2026-08-12"
DEMO_WINDOW_END = "2026-08-25"
