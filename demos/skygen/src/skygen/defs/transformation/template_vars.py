import datetime

import dagster as dg

from skygen.partitions import TENANT_DAILY_PARTITIONS


@dg.template_var
def tenant_daily_partitions():
    return TENANT_DAILY_PARTITIONS


@dg.template_var
def eager_after_blocking_checks():
    """Recompute when upstream changes, but never on an upstream whose blocking check hasn't passed."""
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def sla_freshness():
    """A tenant's scorecard is expected daily: warn at 26h, fail at 36h.
    No SLA window is stated in the brief -- this build's own assumption."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=36),
        warn_window=datetime.timedelta(hours=26),
    )
