"""The single partition dimension in this demo.

Only `fct_bookings_daily` and its downstream Python rollup carry partitions
-- the brief names one axis ("Daily partitioning on the booking-facing
assets -- appointment bookings are naturally daily-cadence"), no second
domain dimension (region, service category) is named, so no
`MultiPartitionsDefinition` is warranted here. `raw_bookings_feed` and the
dbt staging layer stay unpartitioned, matching the pattern used by
demos/partners-fcu (partitioned facts, unpartitioned staging feeding them
via a plain `deps=`/`ref()` edge).

Europe/Stockholm is Bokadirekt's real HQ timezone (Stockholm-based
company, confirmed in the brief's public-signals research) -- unlike most
prior builds' timezone assumptions, this one is not a guess.

Open-ended (no `end_date`) so it always extends through "today".
"""

import dagster as dg

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="Europe/Stockholm",
)
