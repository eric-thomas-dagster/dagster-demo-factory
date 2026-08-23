import os
from pathlib import Path

from dagster import definitions, load_from_defs_folder

# dagster-dbt runs `dbt` from a cached copy of dbt_project/ under
# defs/.local_defs_state/, and a deployed PEX has no "project root" at all
# (only this package's installed files exist) -- so a path relative to
# dbt_project/profiles.yml, or to a repo checkout, doesn't reliably resolve.
# Anchor on this file's own package directory instead: it resolves correctly
# both in local dev (src/northwind_logistics/) and once deployed
# (site-packages/northwind_logistics/). A deployment can still override this
# by setting NORTHWIND_DUCKDB_PATH itself.
os.environ.setdefault(
    "NORTHWIND_DUCKDB_PATH", str(Path(__file__).resolve().parent / "demo_data" / "warehouse.duckdb")
)


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
