"""Shared demo-mode warehouse location.

Real mode targets BigQuery -- Noel referred to "this GCP project" on the
9/9 support call, medium-confidence per the brief. Demo mode targets a
local DuckDB file at this path so dbt (real, not mocked) and every
check/observation function in this project read the same warehouse.

The path defaults to somewhere inside the project
(`demo_data/demo.duckdb`), created on first use, so the project runs with
zero setup after `git clone` + `uv sync` + `dg dev` -- no env var has to be
set by hand. `BOKADIREKT_DEMO_DUCKDB_PATH` can override it, but never
gates startup (see `bokadirekt/__init__.py`).
"""

import os
from pathlib import Path

DEFAULT_DUCKDB_PATH = str(Path(__file__).parent / "demo.duckdb")


def demo_duckdb_path() -> str:
    path = os.environ.get("BOKADIREKT_DEMO_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path
