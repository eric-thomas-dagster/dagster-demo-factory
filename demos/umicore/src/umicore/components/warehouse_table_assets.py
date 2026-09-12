"""Asset-body factory for the two business-unit raw data products with no
ingestion tool named in the brief (`recycling`, `specialty_materials` -- the
AE notes mark per-BU ingestion "unknown", so nothing is invented). Writes
real, deterministic stub rows straight into the shared DuckDB warehouse --
not through a vendor IO manager, since no vendor is claimed for these two.

Same registry gap recorded for demos/detroit-dwsd
(`component-feedback/2026-08-28-graph-first-assets.md`) and reused for
demos/noleggiare and demos/partners-fcu with real I/O: the registry has no
component for "declare a list of assets from YAML with a shared trivial body
writing a stub table," because that's a generic authoring convenience, not
an integration domain -- rungs 1-3 don't apply to it.

One instance covers both raw tables in `defs/recycling/defs.yaml` and
`defs/specialty_materials/defs.yaml` -- adding Umicore's next
no-named-tool business unit is one more `assets:` entry, never another
component instance or another Python file.
"""

import dagster as dg
from pydantic import Field

from umicore.demo_data.bu_stub_data import stub_dataframe
from umicore.demo_data.write_raw_table import write_raw_table

PARTITION_COLUMN_BY_TABLE = {
    "raw.recycling_batch_yield": "processed_on",
    "raw.specialty_materials_output": "produced_on",
}


class WarehouseTableAssetsComponent(dg.Component, dg.Resolvable, dg.Model):
    """Materializes each declared `AssetSpec` with a stub-data body that
    writes a deterministic row set into `raw.<table>` in the shared DuckDB
    warehouse. Asset keys, deps, partitions, checks, and metadata all come
    from the spec itself; `row_count` and `table_name` are the only fields
    this component adds per asset.
    """

    assets: list[dg.ResolvedAssetSpec]
    table_names: dict[str, str] = Field(
        description="Map of asset key (dot-joined) to the raw.<table> it writes.",
    )
    row_count: int = Field(
        default=60,
        description="Deterministic stub row count per partition.",
    )

    @staticmethod
    @dg.template_var
    def daily_partitions() -> dg.DailyPartitionsDefinition:
        from umicore.components.partitions import DAILY_PARTITIONS_DEF

        return DAILY_PARTITIONS_DEF

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[self._build_asset(spec) for spec in self.assets])

    def _build_asset(self, spec: dg.AssetSpec) -> dg.AssetsDefinition:
        op_name = "_".join(spec.key.path)
        asset_key_str = spec.key.to_user_string().replace("/", ".")
        table_name = self.table_names[asset_key_str]
        partition_column = PARTITION_COLUMN_BY_TABLE[table_name]
        row_count = self.row_count

        @dg.multi_asset(specs=[spec], name=op_name)
        def _materialize(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            partition_date = context.partition_key
            frame = stub_dataframe(table_name, partition_date, row_count)
            written = write_raw_table(table_name, frame, partition_column, partition_date)
            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": written,
                    "source": dg.MetadataValue.text(
                        "stubbed synthetic rows -- graph-first fidelity per the brief; no "
                        "ingestion tool is named for this business unit in the AE notes, so "
                        "none is claimed here (kinds reflect the confirmed cloud only)"
                    ),
                }
            )

        return _materialize
