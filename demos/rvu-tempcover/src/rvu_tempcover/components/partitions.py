"""The single partition dimension in this demo.

Only the two daily fact tables carry partitions (per the brief -- no second
domain axis like region or brand is named for this rebuild, so no
`MultiPartitionsDefinition` is warranted here). `Europe/London` matches
Tempcover's home market.

Open-ended (no `end_date`) so it always extends through "today".
"""

import dagster as dg

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="Europe/London",
)
