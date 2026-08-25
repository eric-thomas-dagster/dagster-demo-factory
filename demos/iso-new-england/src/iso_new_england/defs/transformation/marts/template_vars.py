"""Named AutomationCondition/FreshnessPolicy objects for this defs.yaml.

Jinja doesn't support Python's `&`, so the composite condition is built here.
"""

import datetime

import dagster as dg


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def platform_status_freshness_policy():
    """The asset Andrew would page someone about -- his "communication
    mechanism for users" ask, made concrete as a freshness policy.
    """
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=30),
        warn_window=datetime.timedelta(hours=24),
    )
