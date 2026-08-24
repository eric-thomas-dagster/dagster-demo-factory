"""Subclass of `dagster_dbt.DbtProjectComponent` with two fixes.

Unlike the ingestion components, this one needs no demo-mode branch at all:
dbt runs for real against the local DuckDB file in both modes (per the
brief -- "Real dbt execution is strongly preferred over mocked"). Flipping
to a live warehouse is a `profiles.yml` target change, not a code change.

1. **Kind badge.** dagster-dbt derives kinds from the manifest's
   `adapter_type`, so a DuckDB-backed project badges every model `duckdb` by
   default -- there is no `get_kinds` hook to opt out of that. Overriding
   `get_asset_spec()` and calling `replace_attributes(kinds=...)` is the only
   way to badge these models to match Northwind's real warehouse (Snowflake)
   instead of the demo engine.

2. **Partition mapping.** `carrier_rate_validated` depends on `raw/carrier_rate_raw`,
   which is multi-partitioned (day x carrier), but `carrier_rate_validated` is
   only partitioned by day. Dagster's default same-key partition mapping
   would look for a literal day-only key inside a multi-partitioned upstream
   and never find one, so the dependency needs an explicit
   `MultiToSingleDimensionPartitionMapping` telling it to depend on all four
   carrier partitions for its day.
"""

import dagster as dg
from dagster_dbt import DbtProjectComponent

_CARRIER_RATE_RAW_KEY = dg.AssetKey(["raw", "carrier_rate_raw"])
_CARRIER_RATE_VALIDATED_UNIQUE_ID_SUFFIX = ".carrier_rate_validated"


class NorthwindDbtComponent(DbtProjectComponent):
    """`DbtProjectComponent` with a Snowflake kind badge and a partition-mapping fix."""

    def get_asset_spec(self, manifest, unique_id: str, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        spec = spec.replace_attributes(kinds={"dbt", "snowflake"})

        if not unique_id.endswith(_CARRIER_RATE_VALIDATED_UNIQUE_ID_SUFFIX):
            return spec

        new_deps = [
            dg.AssetDep(
                _CARRIER_RATE_RAW_KEY,
                partition_mapping=dg.MultiToSingleDimensionPartitionMapping(partition_dimension_name="date"),
            )
            if dep.asset_key == _CARRIER_RATE_RAW_KEY
            else dep
            for dep in spec.deps
        ]
        return spec.replace_attributes(deps=new_deps)
