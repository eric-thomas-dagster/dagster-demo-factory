"""Demo-mode seam on top of the REAL registry SSIS component.

`SsisWorkspaceComponent` (imported unmodified) is the community-registry
component installed with `dagster-component add ssis_workspace`; it discovers
packages deployed to a SQL Server SSISDB catalog and observes their
executions. This module fakes exactly one network boundary --
`_build_engine()`, the single method discovery and the observation sensor
call to get a SQLAlchemy engine -- so every SQL string the real component
sends runs unmodified against `FakeSsisEngine` (`demo_data/legacy_ssis.py`).

`action: noop` is deliberate: this is the coexistence story. SSIS and SQL
Agent stay master for the legacy per-client loads; Dagster observes them, side by side with the
new Fivetran -> dbt -> Sigma path. The legacy estate is deliberately not wired
into that path's lineage: whether SSIS survives into the future state is open.

Gap worked around, recorded in `component-feedback/2026-10-07-ssis-workspace-sensor-key.md`:
the observation sensor re-derives `[*prefix, folder, project, package]` for the
asset key instead of calling `get_asset_spec()`, so a subclass that overrides
the key silently splits sensor identity from asset identity. The sensor is
rebuilt here to route through `get_asset_spec`.
"""

from typing import Any

import dagster as dg
from dagster._annotations import public
from pydantic import Field

from skygen.components.ssis_workspace.component import (
    SSIS_STATUS,
    SsisPackageProps,
    SsisWorkspaceComponent,
)
from skygen.demo_data.legacy_ssis import FakeSsisEngine

# package_name (".dtsx" already stripped by the parent) -> (flat asset key, domain)
_FLAT_KEY_BY_PACKAGE: dict[str, tuple[str, str]] = {
    "client_database_nightly_load": ("legacy_client_database_nightly_load", "client_operational_data"),
    "client_sla_report_export": ("legacy_client_sla_report_export", "client_sla_reporting"),
}

LEGACY_LOAD_KEY = dg.AssetKey(["legacy_client_database_nightly_load"])


class DemoSsisWorkspaceComponent(SsisWorkspaceComponent):
    """`SsisWorkspaceComponent` that discovers and observes a simulated SSISDB.

    Set `demo_mode: false` and supply the real component's own `workspace`
    connection fields (unchanged) to point this at Skygen's real SQL Server.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Discover and observe a simulated SSISDB instead of a live SQL Server. "
            "Set false and supply `workspace.server`/`user`/`password` to run against "
            "the real SSIS estate."
        ),
    )

    def _build_engine(self) -> Any:
        if not self.demo_mode:
            return super()._build_engine()
        return FakeSsisEngine()

    @public
    def get_asset_spec(self, props: SsisPackageProps) -> dg.AssetSpec:
        """Public hook per the workspace-component convention: flat keys, the
        legacy-chain dependency, and the coexistence narrative metadata."""
        base = super().get_asset_spec(props)
        flat_key, domain = _FLAT_KEY_BY_PACKAGE.get(
            props.package_name, (props.package_name.removesuffix(".dtsx"), "legacy_estate")
        )
        deps = [LEGACY_LOAD_KEY] if flat_key == "legacy_client_sla_report_export" else []
        return base.replace_attributes(
            key=dg.AssetKey([flat_key]),
            group_name="legacy_ssis_estate",
            owners=["team:skygen-dwa"],
            kinds={"ssis", "sqlserver"},
            deps=deps,
            metadata={
                **base.metadata,
                "integration_pattern": "coexistence",
                "legacy_system_boundary": "legacy_scheduler_master_dagster_observes",
                "scheduler_owner": "SQL Agent / SSIS + PowerShell (current)",
                "owner": "Skygen Data Warehouse & Analytics",
                "owner_team": "team:skygen-dwa",
                "tier": "tier_1",
                "domain": domain,
                "deployment_mode": "on-prem SQL Server (current)",
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
