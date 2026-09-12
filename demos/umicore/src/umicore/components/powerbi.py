"""Demo-mode subclass of the native `dagster_powerbi.PowerBIWorkspaceComponent`.

Rung 1 of the escalation ladder: `dagster-powerbi` is a native Dagster
integration (`dagster-component search "power bi" --json` returns zero
registry hits -- confirmed), so this is the real component, subclassed per
rung 3 for the demo-mode I/O seam, following the workspace-component
convention read from its source: `@public` class, `translation:` field,
`@public get_asset_spec(data)` override hook, `StateBackedComponent`
inheritance with enumeration in `write_state_to_path`.

Represents the reporting layer named in the brief -- "the layer where
staleness is currently first noticed by the business" and one of the four
`In code` bucket items ("Power BI refresh as a downstream observed asset").
The base component only builds an executable asset for the semantic model
backing a report (reports/dashboards are read-only views), so this
component represents the artifact as its refreshable semantic model,
downstream of `corporate_rollup.executive_reporting_extract` -- refreshing
the dataset *is* what updates the mesh-health dashboard a BU or corporate
data-mesh lead opens each morning.

Two seams, both additive over the parent:

1. **Discovery (`write_state_to_path`).** Real mode calls
   `fetch_powerbi_workspace_data()` against the live Power BI REST API. Demo
   mode builds the identical `PowerBIWorkspaceData` shape from one literal
   `PowerBIContentData` instead.
2. **Refresh execution (`build_semantic_model_refresh_asset_definition`).**
   Real mode calls the parent's own `workspace_resource.trigger_and_poll_refresh`,
   unmodified. Demo mode reads the row count already sitting in
   `corporate_rollup.executive_reporting_extract` and reports it as
   materialization metadata.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import dagster as dg
from dagster.components.resolved.base import Resolver
from dagster_powerbi import PowerBIWorkspaceComponent
from dagster_powerbi.translator import (
    PowerBIContentData,
    PowerBIContentType,
    PowerBITranslatorData,
    PowerBIWorkspaceData,
)

from umicore.demo_data.warehouse import demo_duckdb_path

_DEMO_DATASET_ID = "umicore-demo-mesh-health-dataset"
_DEMO_DATASET_NAME = "Data Mesh Health"


@dataclass
class UmicorePowerBIComponent(PowerBIWorkspaceComponent):
    """`PowerBIWorkspaceComponent` with a demo-mode discovery + refresh seam.

    `PowerBIWorkspaceComponent` is `@dataclass`-based (not pydantic
    `dg.Model`-based), so this subclass is a dataclass too, with plain
    dataclass fields.
    """

    demo_mode: Annotated[
        bool,
        Resolver.default(
            description=(
                "Build a fixed one-dataset workspace and simulate its refresh instead of "
                "calling the Power BI REST API. Set false and supply real service-principal "
                "credentials in `workspace:` to run against a live Power BI workspace."
            )
        ),
    ] = True

    async def write_state_to_path(self, state_path: Path) -> None:
        if not self.demo_mode:
            return await super().write_state_to_path(state_path)

        semantic_model = PowerBIContentData(
            content_type=PowerBIContentType.SEMANTIC_MODEL,
            properties={
                "id": _DEMO_DATASET_ID,
                "name": _DEMO_DATASET_NAME,
                "configuredBy": None,
                "sources": [],
                "tables": [],
                "webUrl": f"https://app.powerbi.com/groups/demo/datasets/{_DEMO_DATASET_ID}",
            },
        )
        state = PowerBIWorkspaceData.from_content_data(
            workspace_id=self.workspace.workspace_id, content_data=[semantic_model]
        )
        state_path.write_text(dg.serialize_value(state), encoding="utf-8")

    def get_asset_spec(self, data: PowerBITranslatorData) -> dg.AssetSpec:
        """Documented override hook: remaps the base translator's key to
        `power_bi_mesh_health_dashboard_refresh` and layers on the house-rule
        metadata, keeping the `dagster-powerbi/asset_type` tag intact."""
        spec = super().get_asset_spec(data)
        if data.properties.get("id") != _DEMO_DATASET_ID:
            return spec

        return spec.replace_attributes(
            key=dg.AssetKey("power_bi_mesh_health_dashboard_refresh"),
            deps=[dg.AssetDep(dg.AssetKey(["marts", "executive_reporting_extract"]))],
            group_name="corporate_rollup",
            kinds={"powerbi"},
            owners=["team:umicore-digital-transformation"],
            description=(
                "Power BI 'Data Mesh Health' dashboard -- the artifact business-unit and "
                "corporate leads currently first notice staleness in, per the AE notes "
                "('often ... a business team consuming a stale Power BI report'). Modeled "
                "as the semantic model backing the report: refreshing it is the real "
                "triggerable action underneath the report a business user opens."
            ),
        ).merge_attributes(
            metadata={
                "owner": "Umicore Digital & Transformation",
                "owner_team": "team:umicore-digital-transformation",
                "tier": "tier_1",
                "domain": "corporate_reporting",
                "business_impact": "Read by BU and corporate leads each morning to spot stale mesh data.",
                "demo_mode": self.demo_mode,
            }
        )

    def build_semantic_model_refresh_asset_definition(self, spec: dg.AssetSpec) -> dg.AssetsDefinition:
        if not self.demo_mode:
            return super().build_semantic_model_refresh_asset_definition(spec)

        op_name = "_".join(spec.key.path)

        @dg.multi_asset(specs=[spec], name=op_name)
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            row_count = _executive_reporting_extract_row_count()
            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": row_count,
                    "powerbi/refresh_status": "Completed",
                    "source": dg.MetadataValue.text(
                        "simulated -- set demo_mode: false in defs.yaml to refresh via the "
                        "real Power BI API"
                    ),
                }
            )

        return _asset


def _executive_reporting_extract_row_count() -> Any:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        return conn.execute(
            "select count(*) from main_marts.executive_reporting_extract"
        ).fetchone()[0]
    except duckdb.Error:
        return 0
    finally:
        conn.close()
