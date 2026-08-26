"""Custom component: one asset check backed by a SQL assertion against the
shared demo warehouse.

No registry component fits: `dagster-component search "asset check" "data
quality" "sql assertion"` turns up SQL *transforms* (`sql_transform`) and
dbt-specific test wrappers, nothing that expresses an arbitrary blocking/
warning SQL assertion against an asset already materialized outside dbt.
One class here, instantiated once per check via `defs.yaml` -- not four
hand-rolled `@asset_check` functions.

The query is expected to return exactly one row with a `passed` boolean/int
column plus any number of metric columns, which are surfaced as check
metadata. `{date}` and `{dealer_group}` are substituted from the checked
asset's partition key at run time (the latter is empty for daily-only
assets). Every check in this project reads data the trigger-and-observe
assets already land -- it never talks to Fabric -- so it needs no demo_mode
seam of its own; demo vs. real is entirely upstream of the check.
"""

import dagster as dg
from pydantic import Field

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


class DuckDbAssetCheckComponent(dg.Component, dg.Resolvable, dg.Model):
    """A blocking or warning asset check evaluated by one SQL query."""

    asset_key: str = Field(description="Asset this check is attached to.")
    check_name: str
    description: str
    blocking: bool = Field(default=False, description="Blocking checks gate downstream execution on failure.")
    sql: str = Field(
        description=(
            "SQL returning one row with a `passed` boolean column plus metric columns. "
            "May reference {date} and {dealer_group} placeholders."
        )
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        @dg.asset_check(
            asset=dg.AssetKey(self.asset_key),
            name=self.check_name,
            description=self.description,
            blocking=self.blocking,
        )
        def _check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            date, dealer_group = "", ""
            if context.has_partition_key:
                partition_key = context.partition_key
                if hasattr(partition_key, "keys_by_dimension"):
                    date = partition_key.keys_by_dimension.get("date", "")
                    dealer_group = partition_key.keys_by_dimension.get("dealer_group", "")
                else:
                    date = partition_key

            sql = component.sql.format(date=date, dealer_group=dealer_group)
            conn = connect_with_retry(demo_duckdb_path())
            try:
                result = conn.execute(sql).fetchdf()
            finally:
                conn.close()

            row = result.iloc[0]
            metadata = {col: row[col].item() if hasattr(row[col], "item") else row[col] for col in result.columns if col != "passed"}
            return dg.AssetCheckResult(passed=bool(row["passed"]), metadata=metadata)

        return dg.Definitions(asset_checks=[_check])
