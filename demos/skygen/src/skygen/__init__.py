"""Skygen demo project ("One Definition, 170 Tenants").

Zero-setup rule (CLAUDE.md): a fresh clone must run with no manual env vars.
`SKYGEN_DEMO_DUCKDB_PATH` is the single path the demo-mode Fivetran landing
seam, the asset checks, and dbt's `profiles.yml` all resolve the demo
warehouse from, so it is defaulted here -- at package import, before any
component loads or any dbt subprocess is spawned. Setting it externally still
overrides; it never *gates* startup.
"""

import os
from pathlib import Path

os.environ.setdefault(
    "SKYGEN_DEMO_DUCKDB_PATH",
    str(Path(__file__).parent / "demo_data" / "skygen.duckdb"),
)
