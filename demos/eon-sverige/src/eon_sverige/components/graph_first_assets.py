"""Graph-first, pass-bodied asset factory.

Brief fidelity is graph-first (stubbed): no named integration exists for
E.ON Sverige (confidence on the whole stack is low), so lineage, checks,
freshness, and automation carry the story, not real data -- every asset body
is a no-op. Registry search turned up nothing for "declare a list of no-op
assets from YAML" when this need first came up (City of Detroit DWSD build,
2026-08-28) -- that's a generic authoring need, not an integration domain, so
rungs 1-3 of the component escalation ladder don't apply. Reused verbatim
here rather than re-derived; see
`component-feedback/2026-08-28-graph-first-assets.md` for the original
search record.

One instance of this component covers every asset in a source domain --
adding E.ON's next grid-region or data domain is one more `assets:` entry in
`defs.yaml`, never another component instance or another Python file.
"""

import dagster as dg

GRID_REGIONS = ["se1", "se2", "se3", "se4"]
"""Sweden's four electricity bidding zones (Svenska kraftnät), used as the
region dimension for meter-read partitioning. Public, national convention --
not an E.ON-specific system name, which the brief says to avoid inventing.
"""

DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="Europe/Stockholm",
)

REGION_PARTITIONS = dg.StaticPartitionsDefinition(GRID_REGIONS)

METER_READS_PARTITIONS = dg.MultiPartitionsDefinition(
    {"date": DAILY_PARTITIONS, "region": REGION_PARTITIONS}
)


class GraphFirstAssetsComponent(dg.Component, dg.Resolvable, dg.Model):
    """Materializes each declared `AssetSpec` with a trivial no-op body.

    Asset keys, deps, partitions, metadata, and checks all come from the
    spec itself -- this class contributes no business logic, only the empty
    execution function every graph-first asset needs.
    """

    assets: list[dg.ResolvedAssetSpec]

    @staticmethod
    @dg.template_var
    def daily_partitions() -> dg.DailyPartitionsDefinition:
        """Shared instance so cross-asset partition mappings resolve by identity."""
        return DAILY_PARTITIONS

    @staticmethod
    @dg.template_var
    def meter_reads_partitions() -> dg.MultiPartitionsDefinition:
        """Date x grid-region -- matches the multi-region nature of a national
        smart-meter rollout, shared across the meter-read chain so cross-asset
        partition identity holds.
        """
        return METER_READS_PARTITIONS

    @staticmethod
    @dg.template_var
    def recompute_after_upstream_checks_pass() -> dg.AutomationCondition:
        """Eager, but gated on upstream blocking checks.

        Jinja can't write `eager() & ...` inline (no `&` operator), so the
        composition lives here instead. Used on the customer-switching audit
        log so it never rebuilds off an extract its own upstream check has
        failed -- "downstream refuses to compute on bad input" (CLAUDE.md
        feature floor), and the exact story the EU 2026/855 audit trail needs.
        """
        return dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()

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
