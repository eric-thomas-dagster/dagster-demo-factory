"""Asset-body factory for the two Iceberg-badged raw ingestion assets,
writing real `pyarrow.Table` rows through `demo_iceberg_io_manager.py` --
a subclass of the genuine community-registry `iceberg_io_manager` component
(rung 2 of the escalation ladder; see that module's docstring). This class
supplies no integration of its own -- it is not a stand-in for the
price/corporate-action vendor, and it never appears in `kinds`. Its only job
is declaring, from a YAML list of `AssetSpec`s, the small body every one of
those specs needs so the configured IO manager has something real to write
-- the same generic-authoring-convenience shape as
`WarehouseTableAssetsComponent` (demos/partners-fcu, demos/noleggiare), not
a new registry gap (see that component's own docstring for why rungs 1-3
don't apply to "declare a list of assets from YAML with a shared body").

One instance covers both raw assets in `defs/lakehouse_ingestion/defs.yaml`
-- adding a third target-lakehouse source is one more `assets:` entry, never
another component instance or another Python file.
"""

from typing import Callable

import dagster as dg
import pyarrow as pa
from dagster import AssetExecutionContext

from marketgrader.demo_data.lakehouse import generate_corporate_actions, generate_price_feed

_GENERATORS: dict[str, Callable[[str], pa.Table]] = {
    "raw_price_feed": generate_price_feed,
    "raw_corporate_actions": generate_corporate_actions,
}


class LakehouseTableAssetsComponent(dg.Component, dg.Resolvable, dg.Model):
    """Materializes each declared `AssetSpec` by generating its deterministic
    synthetic frame (`demo_data/lakehouse.py`) and writing it through
    `io_manager_key`. Asset keys, deps, partitions, metadata, and checks all
    come from the spec itself.
    """

    io_manager_key: str = "io_manager"
    assets: list[dg.ResolvedAssetSpec]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[self._build_asset(spec) for spec in self.assets])

    def _build_asset(self, spec: dg.AssetSpec) -> dg.AssetsDefinition:
        asset_name = spec.key.path[-1]
        generator = _GENERATORS.get(asset_name)
        if generator is None:
            raise ValueError(
                f"LakehouseTableAssetsComponent: no generator registered for "
                f"{asset_name!r} in demo_data/lakehouse.py's _GENERATORS map."
            )
        # `.with_io_manager_key()` must be the LAST spec transformation --
        # chaining it before any further `.replace_attributes(metadata=...)`
        # silently drops the io-manager key and reverts the op's output type
        # to `Nothing`, which then fails at materialize time with a
        # DagsterTypeCheckError rather than at `dg check defs`.
        final_spec = spec.with_io_manager_key(self.io_manager_key)

        @dg.multi_asset(specs=[final_spec], name=asset_name)
        def _materialize(context: AssetExecutionContext) -> pa.Table:
            table = generator(context.partition_key)
            context.add_output_metadata({"dagster/row_count": table.num_rows})
            return table

        return _materialize
