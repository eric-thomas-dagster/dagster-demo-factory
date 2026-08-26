"""Subclasses the registry's `FabricWorkspaceComponent` -- rung 3 of the
escalation ladder, not a rewrite.

This is the component this brief's Build Directives section exists to
mandate: the second Stellantis build rejected `fabric_workspace` for three
reasons CLAUDE.md's Rung 3 table now names as *not* disqualifying --
live discovery, source-system asset keys, and no partitions support. Each
is answered here by overriding exactly the seam the objection points at,
never by reimplementing the component:

- **Live discovery** -- `_list_items` is the one network call
  `write_state_to_path` makes. In demo mode it returns a fixed item list
  built from `assets_by_item_name` instead of calling the Fabric REST API.
  Everything downstream of it (state caching, bucketing, filtering) is the
  parent's unmodified code.
- **Asset keys from the source system** -- `assets_by_item_name` is the
  explicit mapping table (this demo's `assets_by_task_key` equivalent,
  following `github.com/eric-thomas-dagster/databricks-workspace-bundles-demo`)
  binding each of SFS's ~700-migration Fabric pipeline items to a
  Dagster asset key, its deps, and its partitioning -- one instance, one
  YAML block, N assets. Adding SFS's 700th pipeline is one more entry in
  that table, not a new Python class.
- **No partitions support** -- the parent's `_build_runnable_asset` has no
  `partitions_def` parameter at all. `build_defs_from_state` is overridden
  here (not `_build_runnable_asset`, since the parent's per-type dataset/
  runnable split doesn't fit a mapping-table-driven build) to read the
  mapping table and pass each entry's `partitions` field through to the
  `@asset` it builds.

`_trigger_item_run` is the second network seam -- the pipeline trigger +
poll call. In demo mode it runs the matching deterministic function from
`demo_data.pipelines` (same run/poll/complete lifecycle a real
trigger-and-observe call would have) instead of hitting the Fabric REST
API. Real mode delegates to the parent's implementation unchanged.

Asset keys, groups, kinds, and the YAML schema are identical whether
`demo_mode` is true or false -- only these two methods differ, per
`templates/demo_mode_pattern.py`.
"""


import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

from stellantis_financial_services.components.fabric_workspace.component import (
    FabricWorkspaceComponent,
)
from stellantis_financial_services.components.partitions import (
    DATE_DEALER_GROUP_PARTITIONS_DEF,
    DAILY_PARTITIONS_DEF,
)
from stellantis_financial_services.demo_data.pipelines import (
    PIPELINE_HANDLERS,
    raw_dealer_floorplan_feed,
)

_PARTITIONS_BY_NAME = {
    "daily": DAILY_PARTITIONS_DEF,
    "daily_dealer_group": DATE_DEALER_GROUP_PARTITIONS_DEF,
}


class FabricAssetMapping(dg.Model, dg.Resolvable):
    """One row of the `assets_by_item_name` mapping table.

    Mirrors the shape of `assets_by_task_key` in the Databricks workspace
    component reference build -- explicit `key` / `deps` / owners-style
    metadata per external item, so the mapping table (not a new Python
    class) is what grows as SFS's migrated pipeline count grows.
    """

    key: str = Field(description="Dagster asset key for this Fabric pipeline (their vocabulary, e.g. 'raw_loan_originations').")
    deps: List[str] = Field(default_factory=list, description="Upstream asset keys with matching partitioning.")
    cross_partition_deps: List[str] = Field(
        default_factory=list,
        description=(
            "Upstream asset keys whose partitions_def has an extra 'dealer_group' "
            "dimension this asset doesn't -- wired with a "
            "MultiToSingleDimensionPartitionMapping on 'date'."
        ),
    )
    partitions: str = Field(default="daily", description="'daily' or 'daily_dealer_group'.")
    kinds: List[str] = Field(default_factory=lambda: ["fabric"], description="Asset kind badges, max 3.")
    group_name: str = Field(default="fabric")
    description: str = Field(default="")


class DemoFabricWorkspaceComponent(FabricWorkspaceComponent):
    """`FabricWorkspaceComponent` that mocks discovery and pipeline triggers
    in demo mode, and adds the explicit mapping + partitions support the
    parent component lacks.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Fake the Fabric REST discovery and pipeline-trigger calls with "
            "deterministic synthetic data instead of hitting a live Fabric "
            "workspace. Set false and supply real workspace credentials to "
            "run against SFS's actual Fabric tenant."
        ),
    )
    demo_seed: int = Field(
        default=20260826,
        description="Seed for synthetic generation -- fixed so repeated demo runs produce identical numbers.",
    )
    assets_by_item_name: Dict[str, FabricAssetMapping] = Field(
        description=(
            "Maps each Fabric workspace item's display name to the Dagster asset "
            "it becomes. One entry per migrated SSIS package / Fabric pipeline -- "
            "adding SFS's next pipeline is one more entry here, not a new class."
        ),
    )

    def _demo_item_id(self, name: str) -> str:
        return hashlib.sha256(f"{self.workspace.workspace_id}:{name}".encode()).hexdigest()[:12]

    def _list_items(self) -> List[dict]:
        """The discovery network seam. Demo mode returns the fixed item list
        from `assets_by_item_name` instead of calling the Fabric REST API --
        everything downstream (`write_state_to_path`'s bucketing/filtering)
        is the parent's unmodified code.
        """
        if not self.demo_mode:
            return super()._list_items()
        return [
            {
                "id": self._demo_item_id(name),
                "displayName": name,
                "type": "DataPipeline",
                "description": mapping.description or f"Fabric pipeline: {name}",
                "workspaceId": self.workspace.workspace_id,
            }
            for name, mapping in self.assets_by_item_name.items()
        ]

    def build_defs_from_state(
        self,
        context: dg.ComponentLoadContext,
        state_path: Optional[Path],
    ) -> dg.Definitions:
        """Builds one asset per `assets_by_item_name` entry from the cached
        discovery state -- overridden (rather than the parent's per-type
        `_build_dataset_asset` / `_build_runnable_asset`) because every item
        in this demo is a mapping-table-driven trigger asset with its own
        partitions and deps, which the parent's import-flag-driven split
        doesn't support.
        """
        rows_by_name: Dict[str, dict] = {}
        if state_path is not None and state_path.exists():
            state = json.loads(state_path.read_text())
            rows_by_name = {row["displayName"]: row for row in state.get("pipelines", [])}

        assets = [
            self._build_pipeline_asset(name, mapping, rows_by_name.get(name))
            for name, mapping in self.assets_by_item_name.items()
        ]
        return dg.Definitions(assets=assets)

    def _build_pipeline_asset(self, name: str, mapping: FabricAssetMapping, row: Optional[dict]):
        item_id = row["id"] if row else self._demo_item_id(name)
        partitions_def = _PARTITIONS_BY_NAME[mapping.partitions]

        deps: List[Any] = [dg.AssetKey.from_user_string(d) for d in mapping.deps]
        deps += [
            dg.AssetDep(
                dg.AssetKey.from_user_string(d),
                partition_mapping=dg.MultiToSingleDimensionPartitionMapping(partition_dimension_name="date"),
            )
            for d in mapping.cross_partition_deps
        ]

        _self = self

        @dg.asset(
            key=dg.AssetKey.from_user_string(mapping.key),
            group_name=mapping.group_name,
            partitions_def=partitions_def,
            deps=deps,
            kinds=set(mapping.kinds),
            description=mapping.description or f"Fabric pipeline trigger: {name}",
            metadata={
                "fabric/item_id": item_id,
                "fabric/item_type": "DataPipeline",
                "fabric/workspace_id": self.workspace.workspace_id,
                "fabric/pipeline_name": name,
            },
        )
        def _pipeline_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            row_count = _self._run_pipeline(name, item_id, context)
            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": row_count,
                    "demo_mode": _self.demo_mode,
                }
            )

        return _pipeline_asset

    def _run_pipeline(self, name: str, item_id: str, context: dg.AssetExecutionContext) -> int:
        """The pipeline trigger + poll network seam. Demo mode runs the
        matching deterministic function in `demo_data.pipelines` -- same
        run/poll/complete lifecycle, mocked network. Real mode delegates to
        the parent's Fabric REST trigger + poll, unchanged.
        """
        if not self.demo_mode:
            result = self._trigger_item_run(item_id, "DataPipeline", context.log)
            context.log.info(f"Fabric pipeline {name} run result: {result}")
            return 0

        if name == "raw_dealer_floorplan_feed":
            dealer_group = context.partition_key.keys_by_dimension["dealer_group"]
            date = context.partition_key.keys_by_dimension["date"]
            return raw_dealer_floorplan_feed(date, dealer_group, self.demo_seed)

        handler = PIPELINE_HANDLERS[name]
        date = context.partition_key
        return handler(date, self.demo_seed)
