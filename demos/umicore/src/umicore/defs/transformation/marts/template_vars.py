"""AutomationCondition + FreshnessPolicy values for the marts defs.yaml."""

import datetime

import dagster as dg

from umicore.components import DAILY_PARTITIONS_DEF


@dg.template_var
def daily_partitions():
    return DAILY_PARTITIONS_DEF


@dg.template_var
def eager():
    return dg.AutomationCondition.eager()


@dg.template_var
def corporate_rollup_freshness():
    """No SLA is named in the brief -- a 24h fail / 18h warn window is this
    build's own assumption of a reasonable overnight-batch cadence (see
    README), matching the first of the brief's two named freshness targets
    ("a freshness policy ... on the corporate rollup")."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=24),
        warn_window=datetime.timedelta(hours=18),
    )
