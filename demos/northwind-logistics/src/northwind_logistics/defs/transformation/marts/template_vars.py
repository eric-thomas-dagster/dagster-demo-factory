"""Named AutomationCondition/FreshnessPolicy objects for this defs.yaml.

Jinja (used for YAML template expressions) doesn't support Python's `&`
operator, so `AutomationCondition.eager() & ...` can't be written inline in
the YAML. Composing it here and exposing it as a template var sidesteps that.
"""

import datetime

import dagster as dg


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    """Eager, but gated on upstream blocking checks -- load-bearing for the
    automated recovery sequence: without the check gate, `carrier_cost_allocation`
    and `margin_by_lane_customer` would recompute the moment `carrier_rate_raw`
    updates, even while `carrier_rate_arrival` is still failing.
    """
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def finance_freshness_policy():
    """The assets Priya's team would page someone about."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=30),
        warn_window=datetime.timedelta(hours=24),
    )
