"""The single partition dimension in this demo.

Daily only -- matches the brief's "at least one team's workflow is down on
any given day" framing. No second dimension (region, tenant, carrier) is
named anywhere in the AE notes for Umicore's four business units, so no
`MultiPartitionsDefinition` is warranted here.

`Europe/Brussels` matches Umicore's Antwerp/Brussels HQ. Open-ended (no
`end_date`) so it always extends through "today".
"""

import dagster as dg

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="Europe/Brussels",
)
