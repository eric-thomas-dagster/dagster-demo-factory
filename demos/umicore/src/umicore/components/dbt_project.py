"""Subclass of `dagster_dbt.DbtProjectComponent` with a kind-badge fix.

No demo-mode branch needed: dbt runs for real against the local DuckDB file
in both modes (real dbt Core, real generated lineage, real tests -- Axis 1
requires this regardless of Axis 2 fidelity). Flipping to Umicore's real
warehouse/lakehouse is a `profiles.yml` target change, not a code change.

dagster-dbt derives kinds from the manifest's `adapter_type`, so a
DuckDB-backed project badges every model `duckdb` by default -- there is no
`get_kinds` hook to opt out of that. Overriding `get_asset_spec()` is the
only way to badge these models `dbt` + `databricks`, matching the brief's
confirmed transformation stack ("dbt (Databricks + dbt Core)").
"""

import dagster as dg
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_dbt import DbtProjectComponent


class UmicoreDbtComponent(DbtProjectComponent):
    """`DbtProjectComponent` with a Databricks kind badge instead of DuckDB."""

    def get_asset_spec(self, manifest, unique_id: str, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(kinds={"dbt", "databricks"})

    @property
    def defs_state_config(self) -> DefsStateConfig:
        base = super().defs_state_config
        op_name = self.op.name if self.op else "default"
        return DefsStateConfig(
            key=f"{base.key}[{op_name}]",
            management_type=base.management_type,
            refresh_if_dev=base.refresh_if_dev,
        )
