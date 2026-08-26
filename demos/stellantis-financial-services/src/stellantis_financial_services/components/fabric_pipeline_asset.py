"""Custom component (rung 4 of the escalation ladder): one Dagster asset that
triggers-and-observes one Fabric pipeline, landing its output in the shared
demo warehouse.

Registry gap: searched "microsoft fabric", "fabric pipeline trigger",
"fabric lakehouse", "fabric workspace" in the community registry.
`fabric_workspace` imports whatever items already exist in a *live* workspace
as unpartitioned assets -- it can't produce a *named*, *partitioned*,
*checked* asset ahead of a real Fabric connection. `fabric_pipeline_trigger_job`
produces a bare op-job + schedule, not a lineage-graph asset. Neither fits
17 assets with our own keys, a `MultiPartitionsDefinition` on one of them,
freshness policies, and asset checks wired to specific assets. The
`fabric_workspace_resource` component (rung 2 -- registry, as-is) supplies
the right building block instead: a thin, demo-mode-able REST client
(`list_items` / `trigger_item_run` / `wait_for_run`), used here as a resource
injected into one custom asset-producing component, per
`templates/demo_mode_pattern.py`'s resource-seam variant. One Python class
here, instantiated 17 times from `defs.yaml` -- not 17 hand-rolled
`@asset` functions.

Follows `templates/demo_mode_pattern.py`: the network seam is
`_trigger_and_observe`, the method that would call the real Fabric API and
read back the landed Delta table. Everything else -- asset key, spec,
partitions, deps, freshness, automation, checks wired downstream -- is
identical whether `demo_mode` is true or false.
"""

from datetime import timedelta
from typing import Optional

import dagster as dg
from pydantic import Field

from stellantis_financial_services.components.partitions import (
    DAILY_PARTITIONS_DEF,
    DATE_DEALER_GROUP_PARTITIONS_DEF,
)
from stellantis_financial_services.demo_data import generators
from stellantis_financial_services.demo_data.warehouse import (
    connect_with_retry,
    demo_duckdb_path,
    upsert_partition,
)

_PARTITIONS_DEFS = {
    "daily": DAILY_PARTITIONS_DEF,
    "multi_date_dealer_group": DATE_DEALER_GROUP_PARTITIONS_DEF,
}


class MultiToSingleDep(dg.Model, dg.Resolvable):
    """A dependency on a `MultiPartitionsDefinition` asset from a
    daily-only asset -- rolls up every value of the upstream's other
    dimension for the shared date. Used once here (`dim_dealer` on
    `raw_dealer_floorplan_feed`, rolling up all four dealer_group regions)."""

    asset_key: str
    dimension_name: str = "date"


class FabricPipelineAssetComponent(dg.Component, dg.Resolvable, dg.Model):
    """One asset that triggers-and-observes one Fabric pipeline.

    Real mode calls the injected `FabricWorkspaceResource` to trigger the
    named Fabric item and poll it to completion. Demo mode generates a
    deterministic synthetic batch instead. Either way the result lands in
    the shared demo warehouse table `{group_name}.{asset_key}`, and asset
    key / spec / partitions / deps / freshness / automation are identical
    in both modes -- only `_trigger_and_observe` differs.
    """

    asset_key: str = Field(description="Flat asset key, e.g. 'raw_loan_originations'.")
    generator_key: str = Field(description="Key into demo_data.generators.GENERATORS.")
    description: str
    group_name: str = Field(description="Also used as the demo warehouse schema name.")
    kinds: list[str] = Field(description="Max 3 kind badges, e.g. ['fabric', 'azure'].")
    pipeline_name: str = Field(description="The Fabric pipeline this asset represents, for metadata/narration.")
    deps: list[str] = Field(default_factory=list, description="Plain upstream asset keys.")
    multi_to_single_dep: Optional[MultiToSingleDep] = Field(
        default=None,
        description="One upstream dep that rolls up a MultiPartitionsDefinition asset's extra dimension.",
    )
    partitions: str = Field(default="daily", description="'daily' or 'multi_date_dealer_group'.")
    dollar_metadata_column: Optional[str] = Field(
        default=None, description="Numeric column to sum and surface as dollar-value metadata."
    )
    freshness_fail_hours: Optional[float] = Field(default=None)
    freshness_warn_hours: Optional[float] = Field(default=None)
    automation_eager: bool = Field(default=False, description="Apply AutomationCondition.eager().")
    retry_max: Optional[int] = Field(default=None, description="Only for genuinely flaky sources.")
    retry_delay_seconds: Optional[int] = Field(default=None)
    fabric_resource_key: str = Field(default="fabric")
    fabric_item_name: Optional[str] = Field(
        default=None, description="Display name of the Fabric item to trigger in real mode."
    )
    fabric_item_type: str = Field(default="DataPipeline")
    demo_mode: bool = Field(default=True)
    demo_seed: int = Field(default=20260826)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _PARTITIONS_DEFS[self.partitions]

        asset_deps: list[dg.AssetDep] = [dg.AssetDep(dg.AssetKey(dep)) for dep in self.deps]
        if self.multi_to_single_dep:
            asset_deps.append(
                dg.AssetDep(
                    dg.AssetKey(self.multi_to_single_dep.asset_key),
                    partition_mapping=dg.MultiToSingleDimensionPartitionMapping(
                        partition_dimension_name=self.multi_to_single_dep.dimension_name
                    ),
                )
            )

        freshness_policy = None
        if self.freshness_fail_hours is not None:
            freshness_policy = dg.FreshnessPolicy.time_window(
                fail_window=timedelta(hours=self.freshness_fail_hours),
                warn_window=timedelta(hours=self.freshness_warn_hours) if self.freshness_warn_hours else None,
            )

        spec = dg.AssetSpec(
            key=dg.AssetKey(self.asset_key),
            description=self.description,
            group_name=self.group_name,
            kinds=set(self.kinds),
            deps=asset_deps,
            partitions_def=partitions_def,
            freshness_policy=freshness_policy,
            automation_condition=dg.AutomationCondition.eager() if self.automation_eager else None,
            metadata={
                "fabric_pipeline_name": self.pipeline_name,
                "demo_mode": self.demo_mode,
            },
        )

        retry_policy = None
        if self.retry_max is not None:
            retry_policy = dg.RetryPolicy(max_retries=self.retry_max, delay=self.retry_delay_seconds or 30)

        component = self

        @dg.multi_asset(
            specs=[spec],
            name=f"trigger_{self.asset_key}",
            required_resource_keys={self.fabric_resource_key},
            retry_policy=retry_policy,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if component.partitions == "multi_date_dealer_group":
                keys = context.partition_key.keys_by_dimension
                event_date, dealer_group = keys["date"], keys["dealer_group"]
            else:
                event_date, dealer_group = context.partition_key, None

            fabric = getattr(context.resources, component.fabric_resource_key)
            frame = component._trigger_and_observe(context, fabric, event_date, dealer_group)

            conn = connect_with_retry(demo_duckdb_path())
            try:
                match = {"as_of_date" if "as_of_date" in frame.columns else _date_column(frame): event_date}
                if dealer_group is not None and "dealer_group" in frame.columns:
                    match["dealer_group"] = dealer_group
                upsert_partition(conn, schema=component.group_name, table=component.asset_key, df=frame, match=match)
            finally:
                conn.close()

            metadata = {
                "dagster/row_count": len(frame),
                "fabric_pipeline_name": component.pipeline_name,
                "fabric_run_status": "Completed",
                "demo_mode": component.demo_mode,
            }
            if component.dollar_metadata_column and component.dollar_metadata_column in frame.columns:
                metadata["total_dollar_value"] = dg.MetadataValue.float(
                    round(float(frame[component.dollar_metadata_column].sum()), 2)
                )
            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])

    def _trigger_and_observe(self, context: dg.AssetExecutionContext, fabric, event_date: str, dealer_group: Optional[str]):
        """The network seam. Real mode triggers the named Fabric item and
        polls it to completion; demo mode fakes the whole round trip."""
        if not self.demo_mode:
            items = fabric.list_items(item_type=self.fabric_item_type)
            match = next((it for it in items if it.get("displayName") == self.fabric_item_name), None)
            item_id = match["id"] if match else self.fabric_item_name
            params = {"date": event_date, **({"dealer_group": dealer_group} if dealer_group else {})}
            instance_url = fabric.trigger_item_run(item_id, self.fabric_item_type, parameters=params)
            result = fabric.wait_for_run(instance_url, log=context.log)
            if result.get("status") != "Completed":
                raise RuntimeError(f"Fabric pipeline '{self.fabric_item_name}' run did not complete: {result}")
            raise NotImplementedError(
                "The Fabric trigger/poll round trip above is real. Reading the resulting Delta "
                "table back from OneLake needs the workspace's own lakehouse SQL endpoint / "
                "storage credentials, which this demo build doesn't have -- wire that read here "
                "when connecting to SFS's actual Fabric workspace."
            )
        if dealer_group is not None:
            return generators.gen_raw_dealer_floorplan_feed(event_date, dealer_group, self.demo_seed)
        return generators.GENERATORS[self.generator_key](event_date, self.demo_seed)


def _date_column(frame) -> str:
    for candidate in ("origination_date", "transaction_date", "pull_date", "event_date", "refresh_date"):
        if candidate in frame.columns:
            return candidate
    raise ValueError(f"No recognized date column in frame with columns {list(frame.columns)}")
