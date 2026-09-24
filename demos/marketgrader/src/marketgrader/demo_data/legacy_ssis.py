"""Mock of the SQL Server host behind SSISDB -- the actual system of record
for every index MarketGrader hasn't migrated off SQL Agent/SSIS yet.

No live SQL Server exists for this demo (AE notes: "do not build real SQL
Server connectivity for the legacy side; it's observed, not executed, and
demo_mode never needs real legacy credentials" -- brief, Native integrations
to use). `FakeSsisEngine` stands in for the SQLAlchemy engine
`SsisWorkspaceComponent._build_engine()` normally returns, and is the single
network-boundary seam `DemoSsisWorkspaceComponent` fakes (see that module).
Every SQL string the real component sends -- discovery, the observation
sensor's completion poll, the per-package freshness check -- runs completely
unmodified against this fake connection; only the outermost "open a
connection to SQL Server" call is replaced, per
`templates/demo_mode_pattern.py`.

Two packages, matching the brief's two named legacy assets exactly:
`legacy_sql_agent_price_ingest` (the feed side) and
`legacy_sql_agent_index_calc` (the calculation side) -- standing in for
"every index not yet migrated," side by side with (not chained into) the new
Iceberg-based pipeline.
"""

import datetime
from typing import Any

PACKAGES: list[dict[str, Any]] = [
    {
        "folder_name": "IndexPipelines",
        "project_name": "LegacyPriceCorpActions",
        "project_id": 1001,
        "package_name": "legacy_sql_agent_price_ingest.dtsx",
        "description": (
            "Nightly SQL Agent job (SSIS package) that ingests price and "
            "corporate-action feeds for every index not yet migrated to the "
            "Iceberg/Polaris lake."
        ),
    },
    {
        "folder_name": "IndexPipelines",
        "project_name": "LegacyIndexCalc",
        "project_id": 1002,
        "package_name": "legacy_sql_agent_index_calc.dtsx",
        "description": (
            "Nightly SQL Agent job (SSIS package) that recomputes index "
            "constituent lists for every index not yet migrated to the "
            "Iceberg/Polaris lake."
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
