"""Mock of the SQL Server host behind SSISDB -- Skygen's legacy per-client loads.

No live SQL Server exists for this demo. `FakeSsisEngine` stands in for the
SQLAlchemy engine `SsisWorkspaceComponent._build_engine()` normally returns,
and is the single network-boundary seam `DemoSsisWorkspaceComponent` fakes.
Every SQL string the real component sends -- discovery, the observation
sensor's completion poll, and the per-package freshness check -- runs
unmodified against this fake connection.

Two packages stand in for the SSIS estate the brief says is being migrated
off: the nightly per-client database load, and the legacy SLA report export
that today is how "customers spot SLA misses first". Both stay under SQL
Agent/SSIS ownership (`action: noop`): Dagster observes them, beside (not
upstream of) the new Fivetran -> dbt -> Sigma path.
"""

import datetime
import hashlib
from typing import Any

PACKAGES: list[dict[str, Any]] = [
    {
        "folder_name": "ClientPipelines",
        "project_name": "ClientDatabaseLoads",
        "project_id": 2001,
        "package_name": "client_database_nightly_load.dtsx",
        "description": (
            "Nightly SSIS package (PowerShell-wrapped, scheduled by SQL Agent) that "
            "loads each client's operational SQL Server database -- the source the "
            "Fivetran connector replicates from."
        ),
    },
    {
        "folder_name": "ClientPipelines",
        "project_name": "ClientSlaReporting",
        "project_id": 2002,
        "package_name": "client_sla_report_export.dtsx",
        "description": (
            "Legacy SSIS package that exports per-client SLA reports to flat files -- "
            "the manual path being replaced by the dbt scorecard and Sigma workbook."
        ),
    },
]


def _discovery_rows() -> list[dict[str, Any]]:
    return [
        {
            "folder_name": pkg["folder_name"],
            "project_name": pkg["project_name"],
            "project_id": pkg["project_id"],
            "package_name": pkg["package_name"],
            "description": pkg["description"],
        }
        for pkg in PACKAGES
    ]


def _sensor_rows(cursor: int) -> list[dict[str, Any]]:
    """One newly-completed execution per tick, rotating through the two
    packages -- same deterministic-rotation shape as
    demos/stellantis-financial-services' `legacy_scheduler_observer`.
    `execution_id` is monotonically increasing so the sensor's own
    `execution_id > :cursor` filter (real, unmodified SQL) behaves exactly
    as it would against a real SSISDB.catalog.executions table."""
    pkg = PACKAGES[cursor % len(PACKAGES)]
    execution_id = cursor + 1
    end_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    return [
        {
            "execution_id": execution_id,
            "folder_name": pkg["folder_name"],
            "project_name": pkg["project_name"],
            "package_name": pkg["package_name"],
            "status": 7,  # Succeeded
            "end_time": end_time,
        }
    ]


def _create_execution_row(params: dict[str, Any]) -> dict[str, Any]:
    """Deterministic `execution_id` for a triggered run, keyed on the
    package so repeated demo runs are stable. Namespaced away from the
    sensor's own execution-id sequence (`_sensor_rows`) -- the two are
    cosmetically separate counters in a real SSISDB too (create_execution
    assigns from a global identity column shared by every trigger source,
    Dagster included; nothing here depends on them lining up)."""
    pkg = str(params.get("pkg", "")).removesuffix(".dtsx")
    digest = int.from_bytes(hashlib.sha256(f"skygen|execute|{pkg}".encode()).digest()[:4], "big")
    return {"execution_id": 900_000 + (digest % 100_000)}


def _execution_status_row() -> dict[str, Any]:
    """Always reports Succeeded (status=7) on the first poll -- no real
    wait, and never a planted failure, per house rules. The real
    `_execute_package` only sleeps between polls when the status isn't yet
    terminal, so returning a terminal status immediately means the
    orchestrated run completes instantly in demo mode, same as it would
    the moment a fast SSIS package actually finishes."""
    return {"status": 7}


def _freshness_row(params: dict[str, Any]) -> dict[str, Any] | None:
    """Always reports a completion 5 minutes ago, for whichever package was
    asked about -- deliberately always-fresh, per house rules (no planted
    failures; the checks are demonstrated green, not triggered red)."""
    del params  # every package reports the same fresh completion in demo mode
    end_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    return {"end_time": end_time}


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def mappings(self) -> "_FakeResult":
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class _FakeConnection:
    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(stmt)
        params = params or {}

        if "FROM SSISDB.catalog.packages" in sql:
            return _FakeResult(_discovery_rows())

        if "FROM SSISDB.catalog.executions" in sql and "end_time IS NOT NULL" in sql:
            cursor = int(params.get("cursor", 0))
            return _FakeResult(_sensor_rows(cursor))

        if "TOP 1 end_time" in sql:
            row = _freshness_row(params)
            return _FakeResult([row] if row else [])

        if "EXEC SSISDB.catalog.create_execution" in sql:
            return _FakeResult([_create_execution_row(params)])

        if "EXEC SSISDB.catalog.start_execution" in sql:
            return _FakeResult([])

        if "SELECT status FROM SSISDB.catalog.executions" in sql:
            return _FakeResult([_execution_status_row()])

        raise ValueError(
            f"FakeSsisEngine: no mock handler for this SSISDB query -- "
            f"add one in demo_data/legacy_ssis.py: {sql[:160]!r}"
        )


class FakeSsisEngine:
    """Drop-in stand-in for the SQLAlchemy `Engine` object
    `SsisWorkspaceComponent._build_engine()` normally returns. Supports only
    the one method the real component ever calls: `.connect()`."""

    def connect(self) -> _FakeConnection:
        return _FakeConnection()
