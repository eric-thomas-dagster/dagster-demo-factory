"""Subclass of `dagster_dbt.DbtProjectComponent` with a kind-badge fix.

No demo-mode branch needed: dbt runs for real against the local DuckDB file
in both modes -- flipping to E.ON's live Databricks warehouse is a
`profiles.yml` target change, not a code change (target: dev = duckdb,
target: prod = databricks). The `dbt-databricks` adapter is listed as a
project dep so the switch is one env-var flip away.

dagster-dbt derives kinds from the manifest's `adapter_type`, so a
DuckDB-backed project badges every model `duckdb` by default -- there is
no `get_kinds` hook to opt out of that. Overriding `get_asset_spec()` is
the only way to badge these models `dbt` + `databricks`, matching E.ON's
target future-state stack.
"""

import dagster as dg
from dagster_dbt import DbtProjectComponent


class EonDbtComponent(DbtProjectComponent):
    """`DbtProjectComponent` with a Databricks kind badge instead of DuckDB."""

    def get_asset_spec(self, manifest, unique_id: str, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(kinds={"dbt", "databricks"})
