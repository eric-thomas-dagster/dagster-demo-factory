from pathlib import Path

import dagster as dg


@dg.template_var
def dbt_project_dir() -> str:
    """Absolute path to dbt_project/, anchored on this file's package directory.

    `context.project_root` resolves to a deployed PEX's site-packages root,
    not a repo checkout, so it can't be used to locate dbt_project/ once
    deployed. This file lives at defs/dbt_project/template_vars.py, two
    levels below the package root (northwind_logistics/), where dbt_project/
    also lives -- that relationship holds in both local dev and deployment.
    """
    return str(Path(__file__).resolve().parents[2] / "dbt_project")


@dg.template_var
def northwind_daily_partitions() -> dg.DailyPartitionsDefinition:
    """Shared daily partitions definition for every dbt model in this project.

    Matches the start date used by the ingestion components (carrier_rate_raw,
    shipment_events_raw) so a dbt model's partition window always lines up
    with the raw data it reads.
    """
    return dg.DailyPartitionsDefinition(start_date="2026-08-17")
