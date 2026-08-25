"""The single partition dimension shared by the whole medallion chain.

The brief asks for a `MultiPartitionsDefinition` of date x vendor_source, but
each bronze asset already names one specific vendor feed (`raw_loan_originations`,
`raw_dealer_floorplan_feed`, ...) -- there is no second value `vendor_source`
would ever take for a given asset, so adding it as a partition dimension would
be a fake axis with exactly one real value, not "a second dimension genuinely
part of their domain" (CLAUDE.md's partition guidance). The vendor is the
asset identity, not a partition. This stays a single `DailyPartitionsDefinition`,
matching SFS's daily-batch vendor-file cadence, and the money-shot recovery
story (rematerialize one dealer's one day, not the other 699 packages) is
already genuinely partition-scoped on that one axis.

Open-ended (no `end_date`) so it always extends through "today".
"""

import dagster as dg

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="America/Detroit",
)
