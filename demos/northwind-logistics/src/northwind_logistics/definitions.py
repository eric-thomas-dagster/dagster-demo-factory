import os
from pathlib import Path

from dagster import definitions, load_from_defs_folder

# dagster-dbt runs `dbt` from a cached copy of dbt_project/ under
# defs/.local_defs_state/, so a path relative to dbt_project/profiles.yml
# does not reliably resolve to this repo's demo_data/ directory. Set an
# absolute default here (before any component loads) so the Dagster-side
# warehouse resource and the dbt profile both land on the same file
# regardless of dbt's working directory. A deployment can still override
# this by setting NORTHWIND_DUCKDB_PATH itself.
os.environ.setdefault(
    "NORTHWIND_DUCKDB_PATH", str(Path(__file__).resolve().parents[2] / "demo_data" / "warehouse.duckdb")
)


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
