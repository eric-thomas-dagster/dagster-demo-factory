"""Named partitions + AutomationCondition + FreshnessPolicy values for this defs.yaml."""

from datetime import timedelta

import dagster as dg

from kapitus.components.partitions import DATE_PRODUCT_PARTITIONS_DEF


@dg.template_var
def multi_partitions_def():
    return DATE_PRODUCT_PARTITIONS_DEF


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def funded_loans_freshness():
    """The money-shot mart -- the direct answer to "how would we know something broke."

    Daily batch cadence; a day's funded-loan mart going more than 30h stale
    is the freshness state the VP Data Technology req's mandate is about.
    """
    return dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=30), warn_window=timedelta(hours=24))
