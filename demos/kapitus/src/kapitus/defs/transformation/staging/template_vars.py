"""Named partitions + AutomationCondition values for this defs.yaml.

Jinja (used for YAML template expressions) doesn't support Python's `&`
operator, and the `dg` template scope doesn't expose `MultiPartitionsDefinition`
directly -- composing both here and exposing them as template vars sidesteps
both limitations, the same pattern used for the automation condition in the
Stellantis Financial Services build.
"""

import dagster as dg

from kapitus.components.partitions import DATE_PRODUCT_PARTITIONS_DEF


@dg.template_var
def multi_partitions_def():
    return DATE_PRODUCT_PARTITIONS_DEF


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    """Eager, but gated on upstream blocking checks passing -- a failed
    bronze completeness check genuinely stops automatic propagation into
    staging, not just the one run that surfaced it.
    """
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
