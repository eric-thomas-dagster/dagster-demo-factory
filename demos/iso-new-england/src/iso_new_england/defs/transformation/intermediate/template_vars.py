import dagster as dg


@dg.template_var
def recompute_after_upstream_and_checks_pass():
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
