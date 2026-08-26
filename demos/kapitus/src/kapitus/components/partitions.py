"""The two partition dimensions shared by the whole lending data flow.

`PRODUCT_LINES` are Kapitus's five real product lines (per the brief) --
a genuine second domain axis, not a fabricated one, so `MultiPartitionsDefinition`
is warranted here (see CLAUDE.md's partition guidance). Every bronze,
staging, and marts asset carries the same (date, product_line) shape, which
is what makes "rerun just the SBA partition for 8/12" a real targeted-recovery
story rather than a talking point.

Open-ended (no `end_date`) so it always extends through "today".
"""

import dagster as dg

PRODUCT_LINES = [
    "term_loan",
    "revenue_based_financing",
    "equipment_financing",
    "sba_loan",
    "line_of_credit",
]

DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="America/New_York",
)

PRODUCT_LINE_PARTITIONS_DEF = dg.StaticPartitionsDefinition(PRODUCT_LINES)

DATE_PRODUCT_PARTITIONS_DEF = dg.MultiPartitionsDefinition(
    {"date": DAILY_PARTITIONS_DEF, "product_line": PRODUCT_LINE_PARTITIONS_DEF}
)
