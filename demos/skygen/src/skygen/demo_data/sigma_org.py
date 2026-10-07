"""Mock of a Sigma organization's REST API response -- the network boundary
`dagster_sigma.SigmaComponent.write_state_to_path` normally crosses.

Two workbooks over the dbt marts. Workbook-to-mart lineage is not in this
payload: `DemoSigmaComponent.get_asset_spec` declares it, because real Sigma
reports a workbook's warehouse tables and Dagster's dbt assets are what those
tables are.
"""

from dagster_sigma.translator import SigmaOrganizationData, SigmaWorkbook

WORKBOOKS = (
    {
        "name": "Client SLA Scorecard",
        "workbookId": "wb-client-sla-scorecard",
        "owner": "bi-team@skygen.example",
    },
    {
        "name": "Client Utilization Overview",
        "workbookId": "wb-client-utilization-overview",
        "owner": "bi-team@skygen.example",
    },
)


def demo_organization_data() -> SigmaOrganizationData:
    return SigmaOrganizationData(
        workbooks=[
            SigmaWorkbook(
                properties={
                    "name": wb["name"],
                    "workbookId": wb["workbookId"],
                    "url": f"https://app.sigmacomputing.com/skygen/workbook/{wb['workbookId']}",
                    "latestVersion": 7,
                    "createdAt": "2026-06-01T12:00:00.000Z",
                    "path": "Skygen/Client Reporting",
                },
                lineage=[],
                datasets=set(),
                direct_table_deps=set(),
                owner_email=wb["owner"],
                materialization_schedules=None,
            )
            for wb in WORKBOOKS
        ],
        datasets=[],
        tables=[],
    )
