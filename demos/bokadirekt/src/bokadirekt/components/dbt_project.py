"""Subclass of `dagster_dbt.DbtProjectComponent` with a kind-badge fix.

No demo-mode branch needed: dbt runs for real against the local DuckDB
file regardless of mode (real dbt Core, real `schema.yml` tests, per house
rules -- Axis 1 never mocks the transformation layer). Flipping to
Bokadirekt's actual BigQuery project is a `profiles.yml` target change,
not a code change.

dagster-dbt derives kinds from the manifest's `adapter_type`, so a
DuckDB-backed project badges every model `duckdb` by default -- there is
no `get_kinds` hook to opt out of that. Overriding `get_asset_spec()` is
the only way to badge these models `dbt` + `bigquery` per the brief's
demo-mode kind guidance (`bigquery` is a medium-confidence assumption --
Noel said "this GCP project" but never named BigQuery directly; flagged in
the README).

This component is instantiated twice against the same `project_dir` (an
unpartitioned staging instance and a partitioned marts instance each
`select` a different slice of the dbt project) -- same
`defs_state_config` disambiguation as demos/partners-fcu, folding the
op name into the defs-state key to avoid `DuplicateDefsStateKeyWarning`.
"""

import dagster as dg
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_dbt import DbtProjectComponent


class BokadirektDbtComponent(DbtProjectComponent):
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
