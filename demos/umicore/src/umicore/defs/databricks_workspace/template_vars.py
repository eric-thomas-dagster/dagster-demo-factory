"""Partition + FreshnessPolicy values for this defs.yaml -- Jinja can't
compose these directly, so they're exposed as template vars (same pattern
used across demos/partners-fcu, demos/rvu-tempcover)."""

import datetime

import dagster as dg

from umicore.components import DAILY_PARTITIONS_DEF


@dg.template_var
def daily_partitions():
    return DAILY_PARTITIONS_DEF


@dg.template_var
def ac_feed_freshness():
    """No SLA is named in the brief for the AC feed -- a 24h fail / 18h warn
    window is this build's own assumption of a reasonable overnight-batch
    cadence (see README), matching the second of the brief's two named
    freshness targets ("a freshness policy ... on the AC daily feed")."""
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=24),
        warn_window=datetime.timedelta(hours=18),
    )
