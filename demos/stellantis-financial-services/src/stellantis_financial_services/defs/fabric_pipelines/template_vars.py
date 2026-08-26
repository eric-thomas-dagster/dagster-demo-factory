"""AutomationCondition + FreshnessPolicy values for `defs.yaml`'s post_processing block."""

from datetime import timedelta

import dagster as dg


@dg.template_var
def recompute_after_upstream():
    """The declarative-automation half of the demo's "replay/backfill" story --
    when a corrected or new vendor file lands, everything downstream recomputes
    without anyone remembering to re-trigger it.
    """
    return dg.AutomationCondition.eager()


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    """Same as above, plus: never recompute on top of a failed blocking check.

    Applied to the two assets directly downstream of this demo's blocking
    checks (`stg_loan_originations` after the bronze completeness check,
    `abs_pool_eligibility` after its own reconciliation check).
    """
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def delinquency_snapshot_freshness():
    """The direct answer to "how would we know something broke" for the
    delinquency-rate mart -- daily batch cadence, pages someone if a day's
    snapshot goes more than 30h stale.
    """
    return dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=30), warn_window=timedelta(hours=24))


@dg.template_var
def abs_pool_freshness():
    """The money-shot terminal asset's freshness policy -- tighter than the
    delinquency snapshot, since a stale ABS pool tape is an investor/rating-
    agency problem, not just an internal one.
    """
    return dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=26), warn_window=timedelta(hours=20))
