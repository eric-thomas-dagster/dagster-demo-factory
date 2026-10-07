"""Shared demo-mode warehouse: one local DuckDB file standing in for Snowflake.

Fivetran's landing seam writes raw tables into it, dbt (real dbt Core) builds
the ingest/transform/published/mart layers inside it, and the asset checks
read it. Several Dagster runs can be in flight at once (a UI backfill), and
DuckDB allows a single writer process, so every touch goes through
`warehouse_lock()` -- an advisory file lock that serialises access across
processes. The lock is held only for the duration of one landing write, one
dbt invocation, or one check query.
"""

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

DEFAULT_DUCKDB_PATH = str(Path(__file__).parent / "skygen.duckdb")


def demo_duckdb_path() -> str:
    path = os.environ.get("SKYGEN_DEMO_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def warehouse_lock() -> Iterator[None]:
    lock_path = Path(demo_duckdb_path()).with_suffix(".lock")
    with lock_path.open("w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


@contextmanager
def connect(*, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the demo warehouse under the lock. Read-only needs the file to exist."""
    with warehouse_lock():
        conn = duckdb.connect(demo_duckdb_path(), read_only=read_only)
        try:
            yield conn
        finally:
            conn.close()
