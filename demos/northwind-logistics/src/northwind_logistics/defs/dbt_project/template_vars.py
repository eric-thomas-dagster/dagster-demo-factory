import dagster as dg


@dg.template_var
def northwind_daily_partitions() -> dg.DailyPartitionsDefinition:
    """Shared daily partitions definition for every dbt model in this project.

    Matches the start date used by the ingestion components (carrier_rate_raw,
    shipment_events_raw) so a dbt model's partition window always lines up
    with the raw data it reads.
    """
    return dg.DailyPartitionsDefinition(start_date="2026-08-17")
