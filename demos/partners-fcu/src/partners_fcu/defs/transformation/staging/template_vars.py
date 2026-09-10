"""AutomationCondition values for the staging defs.yaml.

Jinja (used for YAML template expressions) doesn't support Python's `&`
operator, and the `dg` template scope doesn't expose `AutomationCondition`
composition directly -- exposing the composed value as a template var
sidesteps both limitations (same pattern used by demos/rvu-tempcover).
"""

import dagster as dg


@dg.template_var
def eager():
    return dg.AutomationCondition.eager()


@dg.template_var
def eager_after_member_transactions_check():
    """Gated on `raw_member_transactions`'s blocking completeness check --
    `stg_member_transactions` is that asset's direct (source-mapped)
    dependent, so this is where the check's blocking status genuinely gates
    automatic propagation, one hop downstream of the checked asset. Direct
    reversal of the brief's named pain: Dagster refuses to auto-propagate on
    an incomplete transaction feed instead of needing a hand-built ordering
    workaround.
    """
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def eager_after_loan_originations_check():
    """Same gating, for the loan-originations blocking completeness check."""
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
