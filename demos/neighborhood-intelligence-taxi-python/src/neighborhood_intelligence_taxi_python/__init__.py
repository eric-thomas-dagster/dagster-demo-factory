"""Neighborhood Intelligence (NYC Yellow Taxi) demo -- pure-Python counterfactual.

Zero-setup rule: a fresh clone must run with no manual env vars.
`NIT_DEMO_DUCKDB_PATH` is the single path both the demo-data bootstrap and
every `@asset` function resolve the demo warehouse from, so it is defaulted
here -- at package import, before any Dagster definitions load. Setting it
externally still overrides; it never *gates* startup.
"""

import os
from pathlib import Path

os.environ.setdefault(
    "NIT_DEMO_DUCKDB_PATH",
    str(Path(__file__).parent / "demo_data" / "demo.duckdb"),
)
