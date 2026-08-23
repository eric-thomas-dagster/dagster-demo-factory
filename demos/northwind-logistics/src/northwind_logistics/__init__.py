import os
from pathlib import Path

# Every entry point into this package -- `dg dev`, `dg utils refresh-defs-state`,
# the deployed code server -- imports `northwind_logistics` first, so this is
# the one place guaranteed to run before anything (dbt's profile resolution
# included) reads NORTHWIND_DUCKDB_PATH. A deployed PEX has no "project root"
# at all -- only this package's installed files exist -- so the default is
# anchored on this file's own directory, which resolves correctly both in
# local dev (src/northwind_logistics/) and once deployed
# (site-packages/northwind_logistics/). A deployment can still override this
# by setting NORTHWIND_DUCKDB_PATH itself.
os.environ.setdefault(
    "NORTHWIND_DUCKDB_PATH", str(Path(__file__).resolve().parent / "demo_data" / "warehouse.duckdb")
)
