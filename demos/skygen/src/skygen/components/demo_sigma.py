"""Demo-mode seam on top of the REAL `dagster_sigma.SigmaComponent`.

The component, its translator, and the workbook asset specs are the
unmodified `dagster-sigma` code. Only the network boundary is faked:
`write_state_to_path` -- where the component would call Sigma's REST API --
writes a synthetic organization instead. `demo_mode: false` restores the real
call.

`get_asset_spec` (the documented override hook) declares which dbt marts each
workbook reads, so Sigma sits downstream of dbt in one lineage graph. Real
Sigma reports a workbook's warehouse tables; the dbt mart assets are those
tables.
"""

from dataclasses import dataclass
from pathlib import Path

import dagster as dg
from dagster._annotations import public
from dagster_sigma import SigmaComponent

from skygen.demo_data.sigma_org import demo_organization_data

# workbook name -> dbt marts it is built on
_MARTS_BY_WORKBOOK: dict[str, list[dg.AssetKey]] = {
    "Client SLA Scorecard": [dg.AssetKey(["mart", "mart_client_sla_scorecard"])],
    "Client Utilization Overview": [dg.AssetKey(["mart", "mart_client_utilization_summary"])],
}


@dataclass
class DemoSigmaComponent(SigmaComponent):
    demo_mode: bool = True

    @public
    def get_asset_spec(self, data) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        marts = _MARTS_BY_WORKBOOK.get(data.properties.get("name", ""), [])
        return spec.replace_attributes(deps=[*spec.deps, *marts])

    async def write_state_to_path(self, state_path: Path) -> None:
        if not self.demo_mode:
            await super().write_state_to_path(state_path)
            return
        state_path.write_text(dg.serialize_value(demo_organization_data()), encoding="utf-8")
