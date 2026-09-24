"""Demo-mode seam on top of the REAL registry SSIS component.

`SsisWorkspaceComponent` (imported below, unmodified) is the genuine
community-registry component installed via
`dagster-component add ssis_workspace` -- `src/marketgrader/components/ssis_workspace/`
-- it discovers packages deployed to a SQL Server SSISDB catalog and can
optionally trigger + poll them. This module does not reimplement it; per
`templates/demo_mode_pattern.py`, it fakes exactly one network boundary:
`_build_engine()`, the single method every other method on the parent class
(discovery, the observation sensor's completion poll, the per-package
freshness check) calls to get a SQLAlchemy engine. Every one of those
methods runs completely unmodified in demo mode -- same discovery SQL, same
sensor SQL, same freshness-check SQL -- against `FakeSsisEngine`
(`demo_data/legacy_ssis.py`) instead of a live SQL Server connection.

`action: noop` (the parent's own default) is used unchanged: these two
packages are external assets Dagster only ever observes, never triggers --
"SQL Agent stays master for these until they're migrated" (brief). Nothing
about `action: execute` needs a demo-mode seam here because this demo never
uses it.

Two real gaps found reading the component, both worked around here and
both recorded in
`component-feedback/2026-09-24-ssis-workspace-gaps.md`:

1. **No `partitions_def` field at all** -- the base translator's
   `get_asset_spec` builds an unpartitioned `AssetSpec` unconditionally, and
   the observation sensor's `AssetObservation`s carry no `partition=`. Not
   worked around (nothing in this demo needs the legacy assets partitioned --
   they're observed standalone, never chained into the new pipeline), but
   real for a prospect whose legacy packages genuinely run per-day/per-region.
2. **The observation sensor ignores `get_asset_spec`'s key override** -- it
   independently reconstructs `[*prefix, folder_name, project_name, pkg_name]`
   instead of calling `get_asset_spec()` for the key, so a subclass (like
   this one) that overrides `get_asset_spec` to produce a different key
   silently breaks sensor/asset identity: observations land on a key nothing
   in the graph has. Same shape as the Azure Data Factory finding from
   demos/rvu-tempcover. Worked around below by overriding
   `_build_observation_sensor` to build the key the same way `get_asset_spec`
   does, and locked in by `validate_e2e.py`'s
   `sensor and asset agree on identity` assertion.
3. **The `freshness_lag_threshold_seconds` check can't be evaluated through a
   job when `action: noop`** -- `build_defs_from_state` builds `assets = list(specs)`
   (bare `AssetSpec`s, no compute) for `action: noop`, so
   `Definitions.resolve_implicit_job_def_def_for_assets([key])` on a
   check-only selection over one of these keys raises
   `DagsterInvalidDefinitionError: Selected keys must be a subset of
   existing executable asset keys`. Not a demo_mode-only concern -- it would
   bite a real `action: noop` deployment identically. No subclass fix
   attempted (the check def itself is unaffected; only running it via a job
   is blocked) -- `validate_e2e.py` instead invokes the
   `AssetChecksDefinition` directly via `dg.build_asset_check_context`,
   which works because the check function itself has no dependency on a
   job.
"""

from typing import Any, Optional

import dagster as dg
from dagster._annotations import public
from pydantic import Field

from marketgrader.components.ssis_workspace.component import (
    SSIS_STATUS,
    SsisPackageProps,
    SsisWorkspaceComponent,
)
from marketgrader.demo_data.legacy_ssis import FakeSsisEngine

# package_name (already .dtsx-stripped by the parent's own _discover_packages
# / sensor-side stripping -- both call sites do this before touching
# get_asset_spec) -> (flat asset key, group's "domain" metadata value). Keeps
# the brief's exact two asset names -- `legacy_sql_agent_price_ingest` /
# `legacy_sql_agent_index_calc` -- flat, rather than nested under an
# ssis/<host>/<folder>/<project>/ prefix.
_FLAT_KEY_BY_PACKAGE: dict[str, tuple[str, str]] = {
    "legacy_sql_agent_price_ingest": ("legacy_sql_agent_price_ingest", "price_and_corporate_actions"),
    "legacy_sql_agent_index_calc": ("legacy_sql_agent_index_calc", "index_calculation"),
}


class DemoSsisWorkspaceComponent(SsisWorkspaceComponent):
    """`SsisWorkspaceComponent` that discovers and observes a simulated
    SSISDB instead of a live SQL Server. Set `demo_mode: false` and supply
    the real component's own `workspace` connection fields (unchanged) to
    run this exact component against MarketGrader's real SQL Server.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Discover and observe a simulated SSISDB instead of a live SQL "
            "Server -- no MarketGrader SQL Server credentials exist. Set "
            "false and supply `workspace.server`/`user`/`password` (the "
            "real SsisWorkspaceComponent's own fields, unchanged) to point "
            "this at MarketGrader's real SSIS estate."
        ),
    )

    def _build_engine(self) -> Any:
        if not self.demo_mode:
            return super()._build_engine()
        return FakeSsisEngine()

    @public
    def get_asset_spec(self, props: SsisPackageProps) -> dg.AssetSpec:
        """Public hook, per the workspace-component convention. Composes
        onto the parent's own translation-aware base spec (so a real
        `translation:` YAML block still works in production mode) and then
        flattens the key + adds the coexistence-pattern narrative metadata
        the brief asks for."""
        base = super().get_asset_spec(props)
        flat_key, domain = _FLAT_KEY_BY_PACKAGE.get(
            props.package_name, (props.package_name.removesuffix(".dtsx"), "legacy_estate")
        )
        return base.replace_attributes(
            key=dg.AssetKey([flat_key]),
            group_name="legacy_estate",
            kinds={"mssql", "ssis"},
            metadata={
                **base.metadata,
                "integration_pattern": "coexistence",
                "legacy_system_boundary": "sql_agent_owned",
                "owner": "MarketGrader Engineering",
                "owner_team": "team:marketgrader-engineering",
                "tier": "tier_1",
                "domain": domain,
            },
        )

    def _build_observation_sensor(self, specs: list[dg.AssetSpec], server_host: str):
        """Same query + cursor logic as the parent's sensor -- only the
        asset-key construction changes, to route through `get_asset_spec`
        instead of re-deriving `[*prefix, folder, project, pkg]` (gap #2,
        module docstring)."""
        _self = self

        @dg.sensor(
            name="ssis_workspace_observation_sensor",
            minimum_interval_seconds=self.observation_interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
            asset_selection=dg.AssetSelection.assets(*(s.key for s in specs)),
        )
        def _observation_sensor(context: dg.SensorEvaluationContext):
            cursor_val = int(context.cursor) if context.cursor and context.cursor.isdigit() else 0
            engine = _self._build_engine()
            from sqlalchemy import text as sa_text

            with engine.connect() as conn:
                rows = (
                    conn.execute(
                        sa_text(
                            """
                            SELECT e.execution_id, e.folder_name, e.project_name,
                                   e.package_name, e.status, e.end_time
                            FROM SSISDB.catalog.executions e
                            WHERE e.execution_id > :cursor
                              AND e.end_time IS NOT NULL
                            ORDER BY e.execution_id
                            """
                        ),
                        {"cursor": cursor_val},
                    )
                    .mappings()
                    .all()
                )

            observations = []
            new_cursor = cursor_val
            for r in rows:
                exec_id = int(r["execution_id"])
                new_cursor = max(new_cursor, exec_id)
                pkg_name = r["package_name"] or ""
                if pkg_name.lower().endswith(".dtsx"):
                    pkg_name = pkg_name[:-5]
                props = SsisPackageProps(
                    folder_name=r["folder_name"],
                    project_name=r["project_name"],
                    project_id=0,
                    package_name=pkg_name,
                    description=None,
                    server_host=server_host,
                )
                key = _self.get_asset_spec(props).key
                observations.append(
                    dg.AssetObservation(
                        asset_key=key,
                        metadata={
                            "ssis/execution_id": exec_id,
                            "ssis/status": SSIS_STATUS.get(int(r["status"]), str(r["status"])),
                            "ssis/end_time": str(r["end_time"]),
                        },
                    )
                )
            return dg.SensorResult(asset_events=observations, cursor=str(new_cursor))

        return _observation_sensor
