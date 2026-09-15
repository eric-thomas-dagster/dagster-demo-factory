"""`specialist_utilization_daily` -- the one hand-written Python asset in
this demo, mirroring the Python model Noel's own project already has
alongside his dbt folders (brief: Use case -- "a Python model").

Axis 2 (custom logic bodies) is stubbed per the brief's Fidelity
directive: no specific number here is part of the pitch, so the body does
nothing beyond confirming its inputs are in place -- no synthetic
utilization-rate calculation, no fabricated values. Axis 1 (dbt, the real
integration surface) still runs for real regardless; this asset just
never gets a data-backed body per house rules ("stubbed... bodies are
`pass`").

No external system here, so no kind badge and no component -- a plain
`@asset` is exactly what CLAUDE.md carves out for a one-off derived asset
with no integration surface of its own.
"""

import dagster as dg

from bokadirekt.components import DAILY_PARTITIONS_DEF


@dg.asset(
    key="specialist_utilization_daily",
    partitions_def=DAILY_PARTITIONS_DEF,
    deps=[
        dg.AssetDep(dg.AssetKey(["marts", "dim_specialists"])),
        dg.AssetDep(dg.AssetKey(["marts", "fct_bookings_daily"])),
    ],
    group_name="reporting",
    owners=["team:bokadirekt-data"],
    automation_condition=dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed(),
    description=(
        "Per-specialist daily utilization rollup -- stubbed per the brief's "
        "graph-first Fidelity directive; the point is showing a Python asset "
        "living alongside the dbt project, not the number itself."
    ),
    metadata={
        "owner": "Bokadirekt Data",
        "owner_team": "team:bokadirekt-data",
        "tier": "tier_2",
        "domain": "bookings",
        "business_impact": "Would answer 'which specialists are under- or over-booked today' once data-backed.",
    },
)
def specialist_utilization_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "source": dg.MetadataValue.text(
                "stubbed -- graph-first fidelity per the brief; no utilization "
                "calculation is performed"
            )
        }
    )
