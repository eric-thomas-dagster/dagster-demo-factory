"""AutomationCondition values for the staging defs.yaml.

Jinja (used for YAML template expressions) doesn't support Python's `&`
operator, and the `dg` template scope doesn't expose `AutomationCondition`
composition directly -- exposing the composed value as a template var
sidesteps both limitations (same pattern used by demos/partners-fcu,
demos/rvu-tempcover).
"""

import dagster as dg


@dg.template_var
def eager():
    return dg.AutomationCondition.eager()


@dg.template_var
def eager_after_bookings_check():
    """Gated on `stg_bookings`'s own blocking completeness check --
    direct reversal of the account's blocker: instead of a hand-sequenced
    branch-deploy workaround, Dagster refuses to auto-propagate on an
    incomplete bookings feed.
    """
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
