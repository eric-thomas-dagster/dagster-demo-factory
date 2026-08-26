"""FabricPipelineResource -- the trigger-and-observe seam every bronze/silver/
gold/reporting asset in this project uses.

This is the demo's answer to the AE's explicit ask: "evaluating whether
Dagster can become the production orchestration layer on top of (or
replacing) that homegrown system." Dagster does not recompute SFS's ~700
migrated SSIS packages -- it triggers the Fabric pipeline each one has become,
and observes the result. Every asset injects this one resource rather than
calling Fabric directly.

Real mode wraps the community-registry `fabric_workspace_resource` component
(`FabricWorkspaceResource`, added as-is -- rung 2 of the component escalation
ladder -- in `components/fabric_workspace_resource/`) and calls its
`trigger_item_run` / `wait_for_run` REST sequence unmodified. Demo mode fakes
only that one network-crossing call, per `templates/demo_mode_pattern.py`
Rule 1: the run "completes" locally and immediately, so every asset built on
this resource behaves identically in both modes except for where the Fabric
pipeline actually executes.
"""

import dagster as dg
from pydantic import Field

from stellantis_financial_services.components.fabric_workspace_resource.component import FabricWorkspaceResource


class FabricPipelineResource(dg.ConfigurableResource):
    """Triggers a Microsoft Fabric Data Pipeline / Notebook / Dataflow run and waits for completion."""

    fabric_workspace: FabricWorkspaceResource
    demo_mode: bool = Field(
        default=True,
        description=(
            "Simulate the Fabric trigger/poll lifecycle locally instead of calling the real "
            "Fabric REST API. Set false, supply `fabric_workspace.workspace_id` for SFS's real "
            "workspace, and set real service-principal credentials to run this against their "
            "actual Fabric tenant -- no other code changes required."
        ),
    )

    def trigger_and_wait(
        self,
        context: dg.AssetExecutionContext,
        pipeline_item_id: str,
        item_type: str = "DataPipeline",
    ) -> dict:
        """The single network-crossing call every asset in this project makes."""
        if not self.demo_mode:
            instance_url = self.fabric_workspace.trigger_item_run(pipeline_item_id, item_type)
            return self.fabric_workspace.wait_for_run(instance_url, log=context.log)

        context.log.info(
            "[demo mode] simulated Fabric %s trigger for item '%s' -- set demo_mode: false and "
            "supply real Fabric credentials to run this against SFS's actual workspace.",
            item_type,
            pipeline_item_id,
        )
        return {"status": "Completed", "demo_mode": True}


class FabricPipelineResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a `FabricPipelineResource` for use by every asset in this project."""

    resource_key: str = Field(default="fabric")
    demo_mode: bool = Field(default=True)
    workspace_id: str = Field(
        default="00000000-0000-0000-0000-000000000000",
        description="SFS's real Fabric workspace GUID. Unused in demo mode.",
    )
    tenant_id_env_var: str = Field(default="AZURE_TENANT_ID")
    client_id_env_var: str = Field(default="AZURE_CLIENT_ID")
    client_secret_env_var: str = Field(default="AZURE_CLIENT_SECRET")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FabricPipelineResource(
            demo_mode=self.demo_mode,
            fabric_workspace=FabricWorkspaceResource(
                workspace_id=self.workspace_id,
                tenant_id_env_var=self.tenant_id_env_var,
                client_id_env_var=self.client_id_env_var,
                client_secret_env_var=self.client_secret_env_var,
            ),
        )
        return dg.Definitions(resources={self.resource_key: resource})
