"""The single partition dimension shared by the whole chain.

Open-ended (no `end_date`) so it always extends through "today" -- exactly
how a real daily-cadence pipeline is partitioned, and what lets
`external_feed_arrival_sensor` fire for whatever day the demo is actually
run on, with no hardcoded future boundary to keep in sync with the meeting
date.

The brief's stated cadence is "hourly / every 3 hours / daily" with no
second business axis evidenced, so this stays a single `DailyPartitionsDefinition`
rather than a `MultiPartitionsDefinition` -- see CLAUDE.md's partition guidance.
"""

import dagster as dg

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-08-11",
    timezone="America/New_York",
)
