from pathlib import Path

from dagster import definitions, load_from_defs_folder

# NORTHWIND_DUCKDB_PATH's default is set in northwind_logistics/__init__.py,
# not here -- some entry points (dg utils refresh-defs-state) load components
# without importing this module.


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
