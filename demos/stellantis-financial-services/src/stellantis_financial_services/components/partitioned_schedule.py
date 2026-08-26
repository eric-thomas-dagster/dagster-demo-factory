"""Custom component: a cron schedule over a partitioned asset job.

No registry component wraps `dg.build_schedule_from_partitioned_job` behind
declarative config -- it's a two-line core-Dagster call, not an integration,
so there was nothing to search the registry for. One class here, instantiated
once per schedule via `defs.yaml`.
"""

import dagster as dg
from pydantic import Field

from stellantis_financial_services.components.partitions import (
    DAILY_PARTITIONS_DEF,
    DATE_DEALER_GROUP_PARTITIONS_DEF,
)

_PARTITIONS_DEFS = {
    "daily": DAILY_PARTITIONS_DEF,
    "multi_date_dealer_group": DATE_DEALER_GROUP_PARTITIONS_DEF,
}


class PartitionedIngestionScheduleComponent(dg.Component, dg.Resolvable, dg.Model):
    """A daily cron schedule over a named selection of partitioned assets.

    All selected assets must share one `partitions_def` (a
    `dg.define_asset_job` requirement), so the multi-partitioned dealer
    floorplan feed gets its own schedule separate from the daily-only
    bronze feeds.
    """

    job_name: str
    description: str
    asset_selection: list[str]
    partitions: str = Field(default="daily", description="'daily' or 'multi_date_dealer_group'.")
    hour_of_day: int = Field(description="Hour (0-23) to run at, in the partitions_def's own timezone.")
    minute_of_hour: int = Field(default=0)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # `build_schedule_from_partitioned_job` rejects `cron_schedule` /
        # `execution_timezone` for a time-partitioned job -- the cron is
        # derived from the partitions_def's own cadence and timezone, and
        # only the hour/minute within that cadence is configurable.
        partitions_def = _PARTITIONS_DEFS[self.partitions]
        job = dg.define_asset_job(
            name=self.job_name,
            selection=[dg.AssetKey(key) for key in self.asset_selection],
            partitions_def=partitions_def,
            description=self.description,
        )
        schedule = dg.build_schedule_from_partitioned_job(
            job, hour_of_day=self.hour_of_day, minute_of_hour=self.minute_of_hour
        )
        return dg.Definitions(jobs=[job], schedules=[schedule])
