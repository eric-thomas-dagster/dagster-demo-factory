"""The one asset in this demo with a real network boundary to fake.

No registry component covers Qlik Cloud (the BI-publish product Noleggiare
actually runs) -- `dagster-component search "qlik" --json` only surfaces
`qlik_compose_*` components, which target Qlik Compose (a data-warehouse
automation product), a different product under the same vendor. Full search
record: `component-feedback/2026-09-03-qlik-cloud-export.md`.

Per `templates/demo_mode_pattern.py`'s rule 1, this is written as a small
component with `demo_mode` as its only demo-specific field rather than a
bare `@asset` function, so flipping to a live Qlik Cloud account is the one
line the rest of this repo's components use -- `demo_mode: false` in
`defs.yaml` plus real credentials. The asset key, deps, and spec are
identical in both modes; only `_publish` (the outermost network call)
branches.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dagster as dg

DEMO_DATA_DIR = Path(__file__).parent.parent / "demo_data" / "qlik_exports"


class QlikCloudExportComponent(dg.Component, dg.Resolvable, dg.Model):
    """Publishes a single asset's data to a Qlik Cloud app.

    `demo_mode: true` (the default) writes a small manifest to
    `demo_data/qlik_exports/` instead of calling the Qlik Cloud REST API --
    there are no Qlik Cloud credentials for this prospect. Set
    `demo_mode: false` and supply `qlik_cloud_tenant_url` /
    `qlik_cloud_api_key` to publish against a real tenant.
    """

    asset_key: str
    deps: list[str]
    group_name: str
    owners: list[str]
    description: str
    demo_mode: bool = True
    qlik_cloud_tenant_url: str = "{{ env.QLIK_CLOUD_TENANT_URL }}"
    qlik_cloud_api_key: str = "{{ env.QLIK_CLOUD_API_KEY }}"
    qlik_app_id: str = "{{ env.QLIK_CLOUD_APP_ID }}"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            deps=self.deps,
            group_name=self.group_name,
            kinds={"qlik"},
            owners=self.owners,
            description=self.description,
            metadata={
                "owner": "Noleggiare/Tomasi Auto BI Team",
                "owner_team": "team:noleggiare-bi",
                "tier": "tier_1",
                "domain": "bi",
                "business_impact": (
                    "The dashboard Finance leadership in both companies reads "
                    "every morning -- a failed publish means yesterday's numbers "
                    "are what's on screen."
                ),
            },
        )

        component = self

        @dg.multi_asset(specs=[spec], name="qlik_cloud_export")
        def _publish(context: dg.AssetExecutionContext) -> None:
            component._publish(context)

        return dg.Definitions(assets=[_publish])

    def _publish(self, context: dg.AssetExecutionContext) -> None:
        """The entire demo/live seam. Everything else -- asset key, deps,
        metadata, group -- comes from the spec above and is identical
        either way.
        """
        if not self.demo_mode:
            self._publish_live(context)
            return

        DEMO_DATA_DIR.mkdir(parents=True, exist_ok=True)
        manifest: dict[str, Any] = {
            "asset": self.asset_key,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "source": "simulated -- set demo_mode: false in defs.yaml to publish to a real Qlik Cloud tenant",
        }
        out_path = DEMO_DATA_DIR / "fact_finance_consolidated_daily.json"
        out_path.write_text(json.dumps(manifest, indent=2))

        context.add_output_metadata(
            {
                "source": dg.MetadataValue.text(
                    "simulated -- set demo_mode: false in defs.yaml to publish to Qlik Cloud"
                ),
                "manifest_path": dg.MetadataValue.path(str(out_path)),
            }
        )
        context.log.info(
            f"qlik_cloud_export: wrote simulated publish manifest to {out_path}"
        )

    def _publish_live(self, context: dg.AssetExecutionContext) -> None:
        """Real Qlik Cloud REST API publish. Not exercised in demo mode --
        no Qlik Cloud credentials exist for this prospect.
        """
        import requests

        response = requests.post(
            f"{self.qlik_cloud_tenant_url}/api/v1/apps/{self.qlik_app_id}/reload",
            headers={"Authorization": f"Bearer {self.qlik_cloud_api_key}"},
            timeout=30,
        )
        response.raise_for_status()
        context.add_output_metadata(
            {
                "source": dg.MetadataValue.text(self.qlik_cloud_tenant_url),
                "qlik_reload_status": dg.MetadataValue.text(str(response.status_code)),
            }
        )
