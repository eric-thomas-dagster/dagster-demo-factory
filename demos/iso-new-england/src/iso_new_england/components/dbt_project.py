"""Subclass of `dagster_dbt.DbtProjectComponent` with a kind-badge fix.

No demo-mode branch needed: dbt runs for real against the local DuckDB file
in both modes, per the brief ("What can be real: dbt Core executing real
models against DuckDB"). Flipping to ISO-NE's live Postgres warehouse is a
`profiles.yml` target change, not a code change.

dagster-dbt derives kinds from the manifest's `adapter_type`, so a
DuckDB-backed project badges every model `duckdb` by default -- there is no
`get_kinds` hook to opt out of that (see LEARNINGS.md). Overriding
`get_asset_spec()` is the only way to badge these models `postgres`, which
here is one of the rare cases where the badge matches ISO-NE's *actual*
target warehouse, not a disguise -- see the brief's Demo Mode section.
"""

import dagster as dg
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_dbt import DbtProjectComponent


class IsoNeDbtComponent(DbtProjectComponent):
    """`DbtProjectComponent` with a Postgres kind badge instead of DuckDB.

    Also disambiguates `defs_state_config`: three instances of this
    component share one `project_dir` (staging/intermediate/marts each
    `select` a different slice of the same dbt project), and the base
    class keys defs state purely by project dir -- which collides across
    all three and triggers `DuplicateDefsStateKeyWarning`. Each instance
    already has a unique `op.name`, so folding that into the key is enough.
    """

    def get_asset_spec(self, manifest, unique_id: str, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(kinds={"dbt", "postgres"})

    @property
    def defs_state_config(self) -> DefsStateConfig:
        base = super().defs_state_config
        op_name = self.op.name if self.op else "default"
        return DefsStateConfig(
            key=f"{base.key}[{op_name}]",
            management_type=base.management_type,
            refresh_if_dev=base.refresh_if_dev,
        )
