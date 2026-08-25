"""Named AutomationCondition + FreshnessPolicy values for this defs.yaml.

Jinja can't express `&` on `AutomationCondition` or construct a
`timedelta`-taking `FreshnessPolicy` cleanly inline, so both are composed
here and exposed as template vars.
"""

from datetime import timedelta

import dagster as dg


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    """Eager, but gated on upstream blocking checks passing -- the ABS pool
    and delinquency snapshot must not silently recompute over a partition
    the bronze completeness check has flagged.
    """
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def delinquency_snapshot_freshness():
    """Daily batch; SFS's collections team would page someone past 26h stale."""
    return dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=26), warn_window=timedelta(hours=20))


@dg.template_var
def abs_pool_freshness():
    """Feeds the 2026 ABS securitization calendar -- tighter than the
    delinquency snapshot since it's the asset investor/rating-agency
    reporting depends on directly.
    """
    return dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24), warn_window=timedelta(hours=18))
