"""Bokadirekt demo project ("Ship the Second Deployment").

Zero-setup rule (CLAUDE.md): a fresh clone must run with no manual env vars.
`BOKADIREKT_DEMO_DUCKDB_PATH` is the single path dbt's `profiles.yml` and
every check/observation function in this project resolve the demo warehouse
from, so it is defaulted here -- at package import, before any component
loads or any dbt subprocess is spawned. Setting it externally still
overrides; it never *gates* startup.
"""

import os
from pathlib import Path

os.environ.setdefault(
    "BOKADIREKT_DEMO_DUCKDB_PATH",
    str(Path(__file__).parent / "demo_data" / "demo.duckdb"),
)

# Seed the simulated upstream booking-platform tables (see
# demo_data/seed.py) before any component, dbt subprocess, or check tries
# to read them -- idempotent, so this is a no-op on every run after the
# first.
from bokadirekt.demo_data.seed import seed_if_needed  # noqa: E402

seed_if_needed()
