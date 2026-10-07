"""The one fixed-time trigger: the nightly per-tenant refresh.

Each tenant's SLA deadline is 06:00 UTC the morning after the report date (no
real SLA is stated in the brief -- this build's own assumption), so the
landing assets are scheduled at 01:00 UTC; the dbt layers and Sigma follow by
eager automation, gated on the blocking checks. One schedule, one run per
tenant, from one definition.
"""

import dagster as dg

from skygen.partitions import TENANT_DAILY_PARTITIONS

nightly_landing_job = dg.define_asset_job(
    name="skygen_nightly_landing_job",
    selection=dg.AssetSelection.groups("fivetran_ingest"),
    partitions_def=TENANT_DAILY_PARTITIONS,
)

nightly_landing_schedule = dg.build_schedule_from_partitioned_job(
    nightly_landing_job,
    hour_of_day=1,
    minute_of_hour=0,
    name="skygen_nightly_landing_schedule",
    description="Lands the previous day for every tenant at 01:00 UTC, ahead of the 06:00 UTC SLA deadline.",
)
