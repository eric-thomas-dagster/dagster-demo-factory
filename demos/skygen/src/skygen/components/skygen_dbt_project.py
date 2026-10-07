"""Subclass of `dagster_dbt.DbtProjectComponent` for the tenant-scoped dbt project.

dbt runs for real (dbt Core against DuckDB) in both modes; flipping to
Snowflake is a `profiles.yml` target change (`SKYGEN_DBT_TARGET=live`), not a
code change. Three small overrides:

* `get_asset_spec` -- dagster-dbt derives `kinds` from the manifest's
  `adapter_type`, so a DuckDB project badges every model `duckdb`. Badge
  `dbt` + `snowflake` instead (the prospect's real destination).
* `get_cli_args` -- scope each partition's `dbt build` to one tenant and one
  day via `--vars`, so rerunning one `(tenant, date)` partition touches only
  that tenant's rows (the models are incremental delete+insert on that key).
* `execute` -- hold the warehouse lock for the duration of the dbt call, since
  parallel runs from a UI backfill would otherwise contend for DuckDB's
  single writer. A no-op concern on a real warehouse.
"""

import json
from collections.abc import Iterator

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProjectComponent

from skygen.demo_data.warehouse import warehouse_lock
from skygen.partitions import split_partition_key


class SkygenDbtProjectComponent(DbtProjectComponent):
    def get_asset_spec(self, manifest, unique_id: str, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(kinds={"dbt", "snowflake"})

    def get_cli_args(self, context: dg.AssetExecutionContext) -> list[str]:
        tenant, day = split_partition_key(context.partition_key)
        return ["build", "--vars", json.dumps({"tenant": tenant, "report_date": day})]

    def execute(self, context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
        with warehouse_lock():
            yield from super().execute(context, dbt)
