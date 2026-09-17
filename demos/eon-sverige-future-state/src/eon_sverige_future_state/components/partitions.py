"""The single partition dimension in this demo.

Every executable asset in the pipeline (Databricks jobs, dbt models,
MLflow scoring, business output) carries the same monthly partition
definition -- E.ON's meter reads land per day, but the analytical grain
is monthly (see brief).

Start date matches the fixture window's first day so both validation
partitions (`2026-04-01`, `2026-05-01`) resolve to non-empty slices.
Open-ended (no `end_date`) so real-mode runs against Databricks Delta
keep picking up new months without a code change.
"""

import dagster as dg

MONTHLY_PARTITIONS_DEF = dg.MonthlyPartitionsDefinition(
    start_date="2026-04-01",
)
