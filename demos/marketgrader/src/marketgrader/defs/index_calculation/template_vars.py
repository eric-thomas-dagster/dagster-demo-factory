"""AutomationCondition + FreshnessPolicy values for this defs.yaml.

Jinja (used for YAML template expressions) doesn't support Python's `&`
operator, and the `dg` template scope doesn't expose `AutomationCondition`
composition directly -- exposing the composed value as a template var
sidesteps both limitations (same pattern used by demos/partners-fcu,
demos/rvu-tempcover).
"""

import datetime

import dagster as dg

from marketgrader.partitions import DAILY_PARTITIONS


@dg.template_var
def daily_partitions() -> dg.DailyPartitionsDefinition:
    return DAILY_PARTITIONS


@dg.template_var
def eager_after_feed_checks() -> dg.AutomationCondition:
    """Recomputes the moment both raw feeds land -- direct build of the AE
    notes' stated flow ("it runs when its inputs are ready, not on a timed
    chain") -- and refuses to fire if either blocking arrival/completeness
    check on its inputs failed."""
    return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()


@dg.template_var
def index_freshness() -> dg.FreshnessPolicy:
    """No SLA is named in the brief for the index calculation itself -- a
    4h fail / 2h warn window is this build's own assumption that the index
    should recompute the same trading day its inputs land (see README).
    Answers "you'd know within minutes if today's ratings didn't update"
    (capability talk track) with a real, native freshness policy rather
    than a metadata field."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=4),
        warn_window=datetime.timedelta(hours=2),
    )
