"""AutomationCondition values for the staging defs.yaml.

Jinja (used for YAML template expressions) doesn't support Python's `&`
operator, and the `dg` template scope doesn't expose `AutomationCondition`
composition directly -- exposing the composed value as a template var
sidesteps both limitations.
"""

import dagster as dg


@dg.template_var
def eager():
    return dg.AutomationCondition.eager()


@dg.template_var
def eager_after_panel_check():
    """Gated on `raw_panel_insurer_feed`'s blocking completeness check --

    `stg_panel_feed` is that asset's direct (source-mapped) dependent, so
    this is where the check's blocking status genuinely gates automatic
    propagation, one hop downstream of the checked asset.
    """
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
