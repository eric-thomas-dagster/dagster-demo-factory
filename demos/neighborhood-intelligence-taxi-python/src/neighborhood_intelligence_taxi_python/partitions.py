"""Shared monthly partitions definition.

Every executable asset in the graph (silver, gold, report) uses this. Matches
the parent project's dbt models' partitioning grain so validate_e2e.py can
reconcile row counts identically across both demos.
"""

from dagster import MonthlyPartitionsDefinition

MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(start_date="2022-01-01")
