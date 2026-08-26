"""demo_mode wrapper around the registry's `fabric_workspace_resource`.

Follows `templates/demo_mode_pattern.py`'s resource-seam variant: subclass the
real `FabricWorkspaceResource`, override only the three methods that cross
the network (`list_items`, `trigger_item_run`, `wait_for_run`), and leave
everything else -- including the real component's own YAML schema -- alone.
`DemoFabricWorkspaceResourceComponent` adds only the `demo_mode` /
`demo_seed` fields on top of the real `FabricWorkspaceResourceComponent`; the
`workspace_id` / auth fields are unchanged, so flipping `demo_mode: false`
and supplying real values in `defs/resources/fabric/defs.yaml` is enough to
point this at a live Fabric workspace.
"""

from typing import Optional

import dagster as dg
from pydantic import Field

from stellantis_financial_services.components.fabric_workspace_resource.component import (
    FabricWorkspaceResource,
    FabricWorkspaceResourceComponent,
)


class DemoFabricWorkspaceResource(FabricWorkspaceResource):
    """`FabricWorkspaceResource` that fakes only the outbound REST calls."""

    demo_mode: bool = True
    demo_seed: int = 20260826

    def list_items(self, item_type: Optional[str] = None) -> list:
        if not self.demo_mode:
            return super().list_items(item_type)
        return []

    def trigger_item_run(self, item_id: str, item_type: str, parameters: Optional[dict] = None) -> str:
        if not self.demo_mode:
            return super().trigger_item_run(item_id, item_type, parameters)
        # Stand in for the job-instance URL a real trigger would return.
        return f"demo://fabric-run/{item_id}"

    def wait_for_run(self, instance_url: str, max_wait_seconds: int = 1800, poll_interval: int = 15, log=None) -> dict:
        if not self.demo_mode:
            return super().wait_for_run(instance_url, max_wait_seconds, poll_interval, log)
        return {"status": "Completed", "instance_url": instance_url}


class DemoFabricWorkspaceResourceComponent(FabricWorkspaceResourceComponent):
    """Registers a `DemoFabricWorkspaceResource` instead of the live one."""

    demo_mode: bool = Field(
        default=True,
        description="Fake the Fabric REST API instead of calling a real workspace.",
    )
    demo_seed: int = Field(default=20260826, description="Seed for deterministic synthetic generation.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DemoFabricWorkspaceResource(
            workspace_id=self.workspace_id,
            tenant_id_env_var=self.tenant_id_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            demo_mode=self.demo_mode,
            demo_seed=self.demo_seed,
        )
        return dg.Definitions(resources={self.resource_key: resource})
