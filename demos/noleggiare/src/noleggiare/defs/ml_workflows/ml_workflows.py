"""The one hand-written custom-logic asset in this build.

`fleet_residual_value_forecast` names no external system -- it is
Raffaele's own ML workflow logic (Axis 2 territory), not an integration
surface, so Axis 1's "always a real component" rule doesn't apply to it.
Per the brief's graph-first Fidelity directive, the body is a no-op
(`-> None`, so Dagster treats the output as `Nothing` and no IO manager
is invoked at all) -- no forecasting library, no synthetic scores. It
still sits inline in the same lineage as the ETL layer above it, which is
the entire point: "how can ML workflows coexist with ETL pipelines,"
exactly as Raffaele has already started building it himself.

Kept as a plain Python asset rather than routed through
`WarehouseTableAssetsComponent`: that component's whole purpose is
supplying a body that writes through a real warehouse IO manager, which
would badge this asset postgres/snowflake it doesn't claim to be. A
single hand-written asset with no named external system is exactly what
CLAUDE.md's Axis 2 carve-out is for.
"""

import dagster as dg


@dg.asset(
    key="fleet_residual_value_forecast",
    deps=["fact_vehicle_sale", "fact_rental_contract"],
    group_name="ml_workflows",
    kinds={"python"},
    owners=["team:noleggiare-bi"],
    automation_condition=dg.AutomationCondition.eager(),
    description=(
        "Forecasted residual value for fleet and inventory vehicles, "
        "trained against realized sale and rental-return outcomes -- sits "
        "inline in the same lineage as the ETL layer, the direct answer to "
        "'how can ML workflows coexist with ETL pipelines,' the way "
        "Raffaele's team has already started building it themselves."
    ),
    metadata={
        "owner": "Noleggiare/Tomasi Auto BI Team",
        "owner_team": "team:noleggiare-bi",
        "tier": "tier_2",
        "domain": "ml",
        "deployment_mode": "demo (graph-first, no-op body -- Axis 2 custom logic, not an integration)",
        "business_impact": (
            "Informs fleet de-fleet timing and dealer trade-in pricing decisions."
        ),
    },
)
def fleet_residual_value_forecast(context: dg.AssetExecutionContext) -> None:
    context.log.info(
        "fleet_residual_value_forecast: graph-first demo asset -- lineage, "
        "automation, and ML/ETL coexistence are the point, not a forecast."
    )
