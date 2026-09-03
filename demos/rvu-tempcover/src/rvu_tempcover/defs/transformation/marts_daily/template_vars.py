"""Named partition + AutomationCondition + FreshnessPolicy values for this
defs.yaml -- same rationale as `transformation/staging/template_vars.py`.
"""

import datetime

import dagster as dg

from rvu_tempcover.components import DAILY_PARTITIONS_DEF


@dg.template_var
def daily_partitions_def():
    return DAILY_PARTITIONS_DEF


@dg.template_var
def eager():
    return dg.AutomationCondition.eager()


@dg.template_var
def daily_freshness():
    """No SLA is named in the brief -- a 24h fail / 18h warn window is this
    build's own assumption of a reasonable overnight-batch cadence, same
    numbers used in demos/trafigura for the same reason (see README).
    """
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=24),
        warn_window=datetime.timedelta(hours=18),
    )
