"""Named partitions + AutomationCondition values for this defs.yaml."""

import dagster as dg

from kapitus.components.partitions import DATE_PRODUCT_PARTITIONS_DEF


@dg.template_var
def multi_partitions_def():
    return DATE_PRODUCT_PARTITIONS_DEF


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
