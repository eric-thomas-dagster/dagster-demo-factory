"""Shared demo-mode warehouse location.

Real mode targets whatever lakehouse/warehouse product actually sits behind
Umicore's confirmed dbt + Databricks transformation layer (Databricks
Lakehouse or Synapse -- never confirmed by the AE, see the brief's "Conflicts
and gaps" section). Demo mode targets a local DuckDB file so dbt (real, not
mocked), the Databricks-orchestrated raw layer, and the generic BU stub
layer all read and write the same warehouse.

The path defaults to somewhere inside the project (`demo_data/demo.duckdb`),
created on first use, so the project runs with zero setup after `git clone`
+ `uv sync` + `dg dev` -- no env var has to be set by hand.
`UMICORE_DEMO_DUCKDB_PATH` can override it, but never gates startup.
"""

import os
from pathlib import Path

DEFAULT_DUCKDB_PATH = str(Path(__file__).parent / "demo.duckdb")


def demo_duckdb_path() -> str:
    path = os.environ.get("UMICORE_DEMO_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path
