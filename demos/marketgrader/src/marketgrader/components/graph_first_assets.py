"""Graph-first, pass-bodied asset factory.

Brief fidelity is graph-first ("Stubbed (default), graph-first... asset
bodies are pass except where real I/O is needed for the blocking checks"):
lineage, checks, freshness, and automation carry the story for everything
downstream of the raw lakehouse layer, not real index-calculation logic.
Registry search turned up nothing for "declare a list of no-op assets from
YAML" -- that's a generic authoring need, not an integration domain, so
rungs 1-3 of the component escalation ladder don't apply. Reused verbatim
from `demos/detroit-dwsd` (`component-feedback/2026-08-28-graph-first-assets.md`
has the original search record) rather than re-deriving the same registry
gap.

One instance covers every asset in a group -- adding MarketGrader's next
downstream reporting asset is one more `assets:` entry in `defs.yaml`, never
another component instance or another Python file.
"""

import dagster as dg
from dagster import AssetExecutionContext


class GraphFirstAssetsComponent(dg.Component, dg.Resolvable, dg.Model):
    """Materializes each declared `AssetSpec` with a trivial no-op body.

    Asset keys, deps, partitions, metadata, and checks all come from the
    spec itself -- this class contributes no business logic, only the empty
    execution function every graph-first asset needs.
    """

    assets: list[dg.ResolvedAssetSpec]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[self._build_asset(spec) for spec in self.assets])

    @staticmethod
    def _build_asset(spec: dg.AssetSpec) -> dg.AssetsDefinition:
        op_name = "_".join(spec.key.path)

        @dg.multi_asset(specs=[spec], name=op_name)
        def _materialize(context: AssetExecutionContext) -> None:
            context.log.info(
                f"{spec.key.to_user_string()}: graph-first demo asset -- "
                "lineage, checks, freshness, and automation are the point "
                "here, not data movement. Axis 2 (custom logic bodies) is "
                "stubbed per the brief's fidelity call."
            )

        return _materialize
