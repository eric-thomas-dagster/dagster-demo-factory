"""Shared demo-mode warehouse location and write helpers.

Real mode targets Postgres (`components/resources.py`, subclassing the
community-registry `postgres_resource` component). Demo mode targets a local
DuckDB file at this path so dbt (real, not mocked) and the ingestion/landing
components read and write the same warehouse.

The path defaults to somewhere inside the project
(`demo_data/demo.duckdb`), created on first use, so the project runs with
zero setup after `git clone` + `uv sync` + `dg dev` -- no env var has to be
set by hand. `ISO_NE_DEMO_DUCKDB_PATH` can override it, but never gates
startup.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd

DEFAULT_DUCKDB_PATH = str(Path(__file__).parent / "demo.duckdb")


def demo_duckdb_path() -> str:
    path = os.environ.get("ISO_NE_DEMO_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path


def connect_with_retry(path: str, attempts: int = 20, delay_seconds: float = 0.5):
    """Opens a DuckDB connection, retrying on lock contention.

    DuckDB allows only one writer on a file at a time. Dagster's multiprocess
    executor can legitimately try to open this same file from more than one
    process within milliseconds of another. Each individual write here is a
    brief create-table-and-insert, so a short retry loop clears the conflict
    instead of forcing the whole demo onto a single-worker executor.
    """
    import duckdb

    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return duckdb.connect(path)
        except duckdb.IOException as error:
            last_error = error
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not acquire a DuckDB connection to {path} after {attempts} attempts") from last_error


def upsert_partition(
    conn,
    schema: str,
    table: str,
    df: pd.DataFrame,
    match: dict[str, str],
    ddl_columns: dict[str, str] | None = None,
) -> None:
    """Replaces just the rows matching `match` (e.g. one day's partition).

    A partitioned ingestion asset re-running one partition must not clobber
    every other partition already in the table.

    `ddl_columns` (`{column: sql_type}`) pins the schema used the first time
    the table is created, so an empty first batch doesn't leave the table
    with the wrong inferred types for every later insert.
    """
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    existing = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()[0]
    if existing == 0:
        if not ddl_columns:
            conn.register("_incoming_df", df)
            conn.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM _incoming_df")
            conn.unregister("_incoming_df")
            return
        columns_sql = ", ".join(f"{col} {sql_type}" for col, sql_type in ddl_columns.items())
        conn.execute(f"CREATE TABLE {schema}.{table} ({columns_sql})")
        if df.empty:
            return
    where_clause = " AND ".join(f"{col} = '{val}'" for col, val in match.items())
    conn.execute(f"DELETE FROM {schema}.{table} WHERE {where_clause}")
    conn.register("_incoming_df", df)
    conn.execute(f"INSERT INTO {schema}.{table} SELECT * FROM _incoming_df")
    conn.unregister("_incoming_df")
