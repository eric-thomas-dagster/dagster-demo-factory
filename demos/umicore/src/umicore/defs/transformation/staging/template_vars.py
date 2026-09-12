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
def eager_after_specialty_materials_check():
    """Gated on `raw_specialty_materials_output`'s blocking arrival check --
    `stg_specialty_materials` is that asset's direct dependent, so this is
    where the check's blocking status genuinely gates automatic
    propagation, one hop downstream of the checked asset. Direct reversal
    of Anthony's named pain: Dagster refuses to auto-propagate on a
    late/incomplete specialty-materials feed instead of the chain sitting
    stale with no automatic recovery."""
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
