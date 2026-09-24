"""Replaces SQL Agent's own nightly trigger for the two legacy SSIS packages
-- per Eric's live direction: "we can take that over from the SQL Agent...
keeping their legacy SSIS if they want for now." Dagster now owns *when*
these packages run; the packages themselves are untouched.

4:00 AM ET is this build's own assumption (no legacy SQL Agent cadence is
named in the brief) -- ahead of the new pipeline's 5:00 AM ET ingestion
schedule (`daily_ingestion_schedule.py`), so the two nightly batches don't
contend, even though they're otherwise independent (different indices).

Plain `dg.ScheduleDefinition` rather than `build_schedule_from_partitioned_job`
-- these two assets are unpartitioned (see the partitions_def gap recorded
in `component-feedback/2026-09-24-ssis-workspace-gaps.md`), so there's no
partitioned job to build a schedule from.
"""

import dagster as dg

legacy_orchestration_job = dg.define_asset_job(
    name="marketgrader_legacy_orchestration_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["legacy_sql_agent_price_ingest"]),
        dg.AssetKey(["legacy_sql_agent_index_calc"]),
    ),
)

legacy_orchestration_schedule = dg.ScheduleDefinition(
    name="marketgrader_legacy_orchestration_schedule",
    job=legacy_orchestration_job,
    cron_schedule="0 4 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description=(
        "Triggers the legacy price-ingest -> index-calc SSIS chain directly "
        "from Dagster, replacing SQL Agent's own nightly trigger. The SSIS "
        "packages themselves are unchanged."
    ),
)
