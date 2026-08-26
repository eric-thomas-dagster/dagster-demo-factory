"""The one fixed-time trigger in this demo: the overnight batch cutoff.

Loan origination, bank statement, and credit bureau batches land overnight;
this schedule is the fixed deadline every downstream mart's eager automation
condition reacts to (`defs/transformation/*/defs.yaml`). That contrast --
one scheduled trigger at the root, declarative automation cascading the rest
-- is the direct answer to the brief's "no unified data management strategy"
pain: nobody has to remember to update five separate Airflow DAG schedules
when a mart's logic changes.
"""

import dagster as dg

daily_bronze_ingestion_job = dg.define_asset_job(
    name="daily_bronze_ingestion_job",
    description="Pulls the day's batch, across every product line, from every bronze feed.",
    selection=dg.AssetSelection.groups("ingestion"),
)

daily_bronze_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    daily_bronze_ingestion_job,
    hour_of_day=6,
    minute_of_hour=0,
    name="daily_bronze_ingestion_schedule",
    description=(
        "Runs at 6am ET, once the overnight loan origination, bank statement, and credit "
        "bureau batches are expected to have landed."
    ),
)
