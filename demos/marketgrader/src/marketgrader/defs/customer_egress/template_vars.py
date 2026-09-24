"""Shared partition definition for this defs.yaml."""

import dagster as dg

from marketgrader.partitions import DAILY_PARTITIONS


@dg.template_var
def daily_partitions() -> dg.DailyPartitionsDefinition:
    return DAILY_PARTITIONS
