"""Subclass of `dagster_dbt.DbtProjectComponent` with a kind-badge fix.

No demo-mode branch needed: dbt runs for real against the local DuckDB file
in both modes (per the brief -- "dbt Core running for real, with real
tests, against DuckDB"). Flipping to RVU's live BigQuery warehouse is a
`profiles.yml` target change, not a code change.

dagster-dbt derives kinds from the manifest's `adapter_type`, so a
DuckDB-backed project badges every model `duckdb` by default -- there is no
`get_kinds` hook to opt out of that. Overriding `get_asset_spec()` is the
only way to badge these models `dbt` + `bigquery`, per the brief's
demo-mode kind guidance (BigQuery is RVU's decided target warehouse).
"""

import dagster as dg
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_dbt import DbtProjectComponent


class RvuDbtComponent(DbtProjectComponent):
    """`DbtProjectComponent` with a BigQuery kind badge instead of DuckDB.

    Also disambiguates `defs_state_config`: this component is instantiated
    twice against the same `project_dir` (an unpartitioned staging+dims
    instance and a daily-partitioned facts instance each `select` a
    different slice of the same dbt project), and the base class keys defs
    state purely by project dir -- which collides across both and triggers
    `DuplicateDefsStateKeyWarning`. Each instance already has a unique
    `op.name`, so folding that into the key is enough.
    """

    def get_asset_spec(self, manifest, unique_id: str, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(kinds={"dbt", "bigquery"})

    @property
    def defs_state_config(self) -> DefsStateConfig:
        base = super().defs_state_config
        op_name = self.op.name if self.op else "default"
        return DefsStateConfig(
            key=f"{base.key}[{op_name}]",
            management_type=base.management_type,
            refresh_if_dev=base.refresh_if_dev,
        )
