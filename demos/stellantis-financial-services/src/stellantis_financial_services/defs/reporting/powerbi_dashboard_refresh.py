"""The exec-facing BI layer -- represented as a single refresh-trigger asset.

Per the brief: building an actual Power BI workspace integration is out of
scope for a one-night build, and no registry component covers a PowerBI
dataset refresh, so this stays a small hand-written asset rather than a
component. Real mode would call the Power BI REST API's dataset refresh
endpoint; demo mode logs the trigger instead.
"""

import dagster as dg

from stellantis_financial_services.components.partitions import DAILY_PARTITIONS_DEF

@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=dg.AssetKey(["reporting", "powerbi_portfolio_dashboard_refresh"]),
            description=(
                "Triggers a refresh of the exec-facing Power BI portfolio dashboard once "
                "the day's portfolio and customer-360 marts are current."
            ),
            group_name="reporting",
            kinds={"powerbi"},
            partitions_def=DAILY_PARTITIONS_DEF,
            deps=[dg.AssetKey(["marts", "fact_loan_portfolio"]), dg.AssetKey(["marts", "customer_360"])],
            automation_condition=dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed(),
        )
    ]
)
def powerbi_portfolio_dashboard_refresh(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    demo_mode = True  # No Power BI workspace credentials in this demo -- see module docstring.
    contract_date = context.partition_key
    if demo_mode:
        context.log.info(
            "Simulated Power BI dataset refresh trigger for %s. Set demo_mode: false and supply "
            "a Power BI workspace + dataset id to call the real refresh API.",
            contract_date,
        )
    return dg.MaterializeResult(metadata={"demo_mode": demo_mode, "contract_date": contract_date})
