import os
from pathlib import Path

from dagster import definitions, load_from_defs_folder

from northwind_logistics.demo_data.warehouse import demo_duckdb_path

# Zero-setup rule: a fresh clone must run with no manual env vars. This sets
# the one env var dbt's profiles.yml and the ingestion components both read
# for the demo DuckDB file, to the same project-relative default, unless the
# caller already set it (e.g. to point at a different file).
os.environ.setdefault("NORTHWIND_DEMO_DUCKDB_PATH", demo_duckdb_path())


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
