"""Reusable partition + automation + freshness values for the dbt layer."""

import datetime

import dagster as dg

from eon_sverige_future_state.components import MONTHLY_PARTITIONS_DEF


@dg.template_var
def monthly_partitions_def():
    return MONTHLY_PARTITIONS_DEF


@dg.template_var
def eager():
    return dg.AutomationCondition.eager()


@dg.template_var
def monthly_freshness():
    """Gold-layer freshness: 32-day fail / 28-day warn window matches the
    monthly-batch cadence."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(days=32),
        warn_window=datetime.timedelta(days=28),
    )


@dg.template_var
def curated_freshness_warning():
    """Curated data product freshness -- modest since the fixture is small."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(days=32),
        warn_window=datetime.timedelta(days=28),
    )
