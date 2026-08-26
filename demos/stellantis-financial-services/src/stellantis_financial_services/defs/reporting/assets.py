"""Reporting layer: the exec-facing Power BI dashboard refresh.

Represented as a single Fabric Dataflow-refresh trigger asset, per the
brief's explicit scope note -- a full embedded Power BI workspace
integration is out of scope for a one-night build. This asset writes
nothing to the warehouse; materializing it triggers the refresh, exactly
like every other asset in this project.
"""

import dagster as dg

from stellantis_financial_services.components.fabric_resource import FabricPipelineResource
from stellantis_financial_services.components.partitions import DATE_PARTITIONS_DEF


@dg.asset(
    key=dg.AssetKey(["reporting", "powerbi_portfolio_dashboard_refresh"]),
    deps=[
        dg.AssetKey(["marts", "abs_pool_eligibility"]),
        dg.AssetKey(["marts", "fact_delinquency_snapshot"]),
        dg.AssetKey(["marts", "gl_reconciliation_summary"]),
        dg.AssetKey(["marts", "customer_360"]),
    ],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name="reporting",
    kinds={"powerbi"},
    automation_condition=dg.AutomationCondition.eager(),
    description=(
        "Refreshes the exec-facing Power BI portfolio dashboard once every mart it reads from "
        "is current. Fabric-native BI layer -- refresh is a Dataflow Gen2 trigger, same "
        "trigger-and-observe lifecycle as every ingestion asset."
    ),
)
def powerbi_portfolio_dashboard_refresh(
    context: dg.AssetExecutionContext, fabric: FabricPipelineResource
) -> dg.MaterializeResult:
    run = fabric.trigger_and_wait(
        context, pipeline_item_id="pl_powerbi_portfolio_dashboard_refresh", item_type="Dataflow"
    )
    return dg.MaterializeResult(metadata={"fabric_run_status": run["status"]})
