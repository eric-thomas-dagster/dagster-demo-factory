"""The single partition dimension in this demo.

Only the ingestion assets and the two daily marts carry partitions -- the
brief names one axis ("daily time partitioning on the transactional/fact
assets"), no second domain dimension (region, product, branch) is named, so
no `MultiPartitionsDefinition` is warranted here.

`America/New_York` is this build's own assumption -- no headquarters
timezone is stated in the brief; Partners FCU is a US credit union, so
Eastern is the most plausible default (see README "Assumptions").

Open-ended (no `end_date`) so it always extends through "today".
"""

import dagster as dg

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="America/New_York",
)
