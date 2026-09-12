"""Umicore demo project ("Untangling the Spiderweb").

Zero-setup rule (CLAUDE.md): a fresh clone must run with no manual env vars.
`UMICORE_DEMO_DUCKDB_PATH` is the single path the demo-mode Databricks and
generic raw layers, and dbt's `profiles.yml`, all resolve the demo warehouse
from, so it is defaulted here -- at package import, before any component
loads or any dbt subprocess is spawned. Setting it externally still
overrides; it never *gates* startup.
"""

import os
from pathlib import Path

os.environ.setdefault(
    "UMICORE_DEMO_DUCKDB_PATH",
    str(Path(__file__).parent / "demo_data" / "demo.duckdb"),
)
