"""Reusable partition + automation + freshness values for the dbt layer.

Exposed as `@dg.template_var`s so the sibling `defs.yaml` composes them
with `{{ name }}` refs. Jinja can't construct `AutomationCondition` or
`FreshnessPolicy` objects inline, so this file is the seam that keeps
the YAML clean.
"""

import datetime

import dagster as dg

from neighborhood_intelligence_taxi.components import MONTHLY_PARTITIONS_DEF


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
def report_freshness_warning():
    """Report-layer freshness: same shape as gold; the brief's checks
    section calls the report freshness 'warning' severity."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(days=32),
        warn_window=datetime.timedelta(days=28),
    )
