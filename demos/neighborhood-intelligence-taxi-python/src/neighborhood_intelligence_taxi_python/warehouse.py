"""Shared demo-mode warehouse location.

Every `@asset` function in this project opens DuckDB at this path via the
`DuckDBResource`. Same file the demo-data bootstrap seeds `raw.*` tables into.

Defaults to `demo_data/demo.duckdb` inside the project so a fresh clone runs
with zero setup. `NIT_DEMO_DUCKDB_PATH` overrides, never gates.
"""

import os
from pathlib import Path

DEFAULT_DUCKDB_PATH = str(Path(__file__).parent / "demo_data" / "demo.duckdb")


def demo_duckdb_path() -> str:
    path = os.environ.get("NIT_DEMO_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path
