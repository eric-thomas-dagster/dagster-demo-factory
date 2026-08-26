"""Kapitus demo project.

Zero-setup rule (CLAUDE.md): a fresh clone must run with no manual env vars.
`KAPITUS_DEMO_DUCKDB_PATH` is the single path both the ingestion components
and dbt's `profiles.yml` resolve the demo warehouse from, so it is defaulted
here -- at package import, before any component loads or any dbt subprocess
is spawned. Setting it externally still overrides; it never *gates* startup.
"""

import os
from pathlib import Path

os.environ.setdefault(
    "KAPITUS_DEMO_DUCKDB_PATH",
    str(Path(__file__).parent / "demo_data" / "demo.duckdb"),
)
