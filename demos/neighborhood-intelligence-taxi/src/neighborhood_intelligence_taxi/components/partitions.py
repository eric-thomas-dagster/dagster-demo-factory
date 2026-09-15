"""The single partition dimension in this demo.

Every asset in the pipeline (bronze external tables, silver + gold + report
dbt models) carries the same monthly partition definition -- yellow-taxi
trip data lands as one BigQuery table per year, but the natural analytics
grain is a month (see brief).

Start date matches the fixture window's first day so both validation
partitions (`2022-01-01`, `2022-02-01`) resolve to non-empty slices.
Open-ended (no `end_date`) so real-mode runs against BigQuery keep
picking up new months without a code change.

Timezone: UTC. The community `cron_schedule` / `external_bigquery_table`
components build their partitions_defs without a timezone kwarg (so
default UTC), and Dagster rejects a job whose partitions_def timezone
differs from its selected assets' partitions_def timezone. Aligning here
on UTC lets the whole pipeline share one partitions_def value across the
scheduler component's implicit build and the assets' explicit template
var.
"""

import dagster as dg

MONTHLY_PARTITIONS_DEF = dg.MonthlyPartitionsDefinition(
    start_date="2022-01-01",
)
