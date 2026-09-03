"""Demo-mode subclass of the native `dagster_powerbi.PowerBIWorkspaceComponent`.

Rung 1 of the escalation ladder: `dagster-powerbi` is a native Dagster
integration (`dagster-component search "power bi" --json` returns zero
registry hits -- confirmed, see `component-feedback/` search record in the
project README and the notification), so this is the real component, not
a registry one, subclassed per rung 3 for the demo-mode I/O seam. It
follows the same workspace-component convention read directly from its
source: `@public` class, `translation:` field, `@public
get_asset_spec(data)` override hook, `StateBackedComponent` inheritance
with enumeration happening in `write_state_to_path`.

RVU's brief names one reporting asset,
`power_bi_quote_performance_report`, downstream of
`fct_bound_policies_daily`. The real, materializable action a Power BI
integration can trigger is a **semantic-model refresh** -- reports and
dashboards are read-only views over a dataset, so the base component only
builds an executable asset for the semantic model backing them
(`build_semantic_model_refresh_asset_definition`; reports/dashboards stay
plain external `AssetSpec`s with no compute body). This component
therefore represents the Power BI artifact as its refreshable semantic
model, named `power_bi_quote_performance_report` -- refreshing the dataset
*is* what updates the report a business user opens, so this is the real
triggerable operation underneath that name, not a stand-in for it.

Two seams, both additive over the parent:

1. **Discovery (`write_state_to_path`).** Real mode calls
   `fetch_powerbi_workspace_data()` against the live Power BI REST API.
   Demo mode builds the identical `PowerBIWorkspaceData` shape from one
   literal `PowerBIContentData` (a semantic model, API-response-shaped)
   instead.
2. **Refresh execution
   (`build_semantic_model_refresh_asset_definition`).** Real mode calls
   the parent's own `workspace_resource.trigger_and_poll_refresh`, the
   real Power BI refresh-and-poll REST loop, unmodified. Demo mode reads
   the row count already sitting in `fct_bound_policies_daily` (the
   dependency this report is built on) and reports it as materialization
   metadata, mirroring the shape a real refresh's output would have.
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

from rvu_tempcover.demo_data.warehouse import demo_duckdb_path

_DEMO_DATASET_ID = "rvu-demo-quote-performance-dataset"
_DEMO_DATASET_NAME = "Quote Performance"


@dataclass
class RvuPowerBIComponent(PowerBIWorkspaceComponent):
    """`PowerBIWorkspaceComponent` with a demo-mode discovery + refresh seam.

    `PowerBIWorkspaceComponent` is `@dataclass`-based (not pydantic
    `dg.Model`-based like `FivetranAccountComponent`), so this subclass is
    a dataclass too, with plain dataclass fields -- the `Resolvable` schema
    is derived from `dataclasses.fields()` for this base class, and a
    pydantic `Field()` default is not recognized by that path.
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
        """The discovery seam. Demo mode builds one API-response-shaped
        semantic model instead of scanning a live workspace."""
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
        """Documented override hook: remaps the base translator's
        `["semantic_model", <cleaned name>]` key to
        `power_bi_quote_performance_report` and layers on the house-rule
        metadata, keeping the `dagster-powerbi/asset_type` tag intact so
        the refreshable-asset path (`_should_spec_be_refreshable`) still
        recognizes this spec.
        """
        spec = super().get_asset_spec(data)
        if data.properties.get("id") != _DEMO_DATASET_ID:
            return spec

        return spec.replace_attributes(
            key=dg.AssetKey("power_bi_quote_performance_report"),
            deps=[dg.AssetDep(dg.AssetKey(["marts", "fct_bound_policies_daily"]))],
            group_name="reporting",
            kinds={"powerbi"},
            owners=["team:rvu-data-platform"],
            description=(
                "Power BI dashboard reporting bound-policy volume and premium by panel "
                "insurer -- the artifact underwriting and finance actually look at each "
                "morning. Modeled as the semantic model backing the report: refreshing "
                "it is the real triggerable action underneath the report a business "
                "user opens."
            ),
        ).merge_attributes(
            metadata={
                "owner": "RVU Data Platform",
                "owner_team": "team:rvu-data-platform",
                "tier": "tier_1",
                "domain": "reporting",
                "business_impact": "Read by underwriting and finance every morning.",
                "demo_mode": self.demo_mode,
            }
        )

    def build_semantic_model_refresh_asset_definition(self, spec: dg.AssetSpec) -> dg.AssetsDefinition:
        """The refresh seam. Real mode delegates to the parent's own
        implementation (the real Power BI trigger-and-poll REST loop).
        Demo mode reads `fct_bound_policies_daily`'s row count and reports
        it, standing in for "the dataset refresh picked up the latest
        fact table"."""
        if not self.demo_mode:
            return super().build_semantic_model_refresh_asset_definition(spec)

        op_name = "_".join(spec.key.path)

        @dg.multi_asset(specs=[spec], name=op_name)
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            row_count = _fct_bound_policies_row_count()
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


def _fct_bound_policies_row_count() -> Any:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        return conn.execute("select count(*) from main_marts.fct_bound_policies_daily").fetchone()[0]
    finally:
        conn.close()
