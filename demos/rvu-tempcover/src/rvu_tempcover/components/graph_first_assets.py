"""Graph-first, pass-bodied asset factory.

Brief fidelity is graph-first for the Fivetran ingestion, Braze activation,
and Power BI reporting layers -- lineage, checks, freshness, and automation
carry the story there, not real data (dbt SQL is where this brief's real
data lives; see `dbt_project.py`). The registry has no component for
"declare a list of no-op assets from YAML" -- that's a generic authoring
need, not an integration domain, so rungs 1-3 of the component escalation
ladder don't apply. Search record and suggested registry addition:
`component-feedback/2026-08-28-graph-first-assets.md` (first written for
demos/detroit-dwsd, reused verbatim for demos/trafigura and here -- same
gap, not re-searched, per LEARNINGS.md).

One instance of this component covers every asset in a source domain --
adding the prospect's next feed is one more `assets:` entry in `defs.yaml`,
never another component instance or another Python file.
"""

import dagster as dg


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
        def _materialize(context: dg.AssetExecutionContext) -> None:
            context.log.info(
                f"{spec.key.to_user_string()}: graph-first demo asset -- "
                "lineage, checks, and automation are the point, not data movement."
            )

        return _materialize
