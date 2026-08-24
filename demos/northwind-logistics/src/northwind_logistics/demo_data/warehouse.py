"""Shared demo-mode warehouse location and write helper.

Real mode targets Snowflake (`dagster-snowflake`, unmodified -- see
`components/resources.py`). Demo mode targets a local DuckDB file at this
path so `dbt` (real, not mocked) and the ingestion components read and write
the same warehouse.

The path lives outside the project directory (system temp, namespaced) so it
survives a `dg dev` reload and works the same whether `dg` runs from the
project root or not. It does **not** survive across separate Dagster+
Serverless runs -- Serverless gives each run its own ephemeral disk -- which
is why `components/dbt_project.py` re-seeds the raw layer from the
deterministic generators at the start of every dbt build in demo mode rather
than assuming a previous run's tables are still there.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import duckdb
import pandas as pd

DEFAULT_DUCKDB_PATH = str(
    Path(tempfile.gettempdir()) / "northwind_logistics_demo" / "warehouse.duckdb"
)


def demo_duckdb_path() -> str:
    path = os.environ.get("NORTHWIND_DEMO_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path


def connect_with_retry(path: str, attempts: int = 20, delay_seconds: float = 0.5) -> duckdb.DuckDBPyConnection:
    """Opens a DuckDB connection, retrying on lock contention.

    DuckDB allows only one writer on a file at a time. `dg launch --assets '*'`
    materializes several ingestion assets through Dagster's multiprocess
    executor, and those processes can legitimately try to open this same file
    within milliseconds of each other. Each individual write here is a brief
    create-table-and-insert, so a short retry loop clears the conflict instead
    of forcing the whole demo onto a single-worker executor.
    """
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return duckdb.connect(path)
        except duckdb.IOException as error:
            last_error = error
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not acquire a DuckDB connection to {path} after {attempts} attempts") from last_error


def write_table(conn, schema: str, table: str, df: pd.DataFrame) -> None:
    """Replaces `schema.table` with `df`'s contents.

    `DemoWarehouseResource.get_connection()` (see `resources.py`) is the only
    thing that decides which warehouse this is -- everything above this
    function (which rows, which schema, when to call it) is identical in
    both modes. This function still has to pick between two wire protocols,
    because DuckDB and the Snowflake Python connector simply don't share one:
    that split is a real driver difference, not a demo-mode branch.
    """
    if "duckdb" in type(conn).__module__:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.register("_incoming_df", df)
        conn.execute(f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM _incoming_df")
        conn.unregister("_incoming_df")
        return

    from snowflake.connector.pandas_tools import write_pandas

    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    write_pandas(
        conn=conn,
        df=df.rename(columns=str.upper),
        table_name=table.upper(),
        schema=schema.upper(),
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=True,
    )


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
    every other partition already in the table -- unlike `write_table`,
    which replaces the whole table and is only safe for small, wholly
    re-fetched snapshots (the three Fivetran-sourced tables).

    `ddl_columns` (`{column: sql_type}`) pins the schema used the first time
    the table is created. It matters specifically because the planted
    anomaly is an *empty* DataFrame: DuckDB can't infer a column's type from
    zero rows of an `object`-dtype pandas column, and if the anomaly
    partition happens to run first, `CREATE TABLE ... AS SELECT` from that
    empty frame silently picks the wrong types for every later insert.
    """
    if "duckdb" in type(conn).__module__:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        existing = conn.execute(
            f"SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        ).fetchone()[0]
        if existing == 0:
            if not ddl_columns:
                conn.register("_incoming_df", df)
                conn.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM _incoming_df")
                conn.unregister("_incoming_df")
                return  # rows are already in the table via CREATE TABLE AS SELECT
            columns_sql = ", ".join(f"{col} {sql_type}" for col, sql_type in ddl_columns.items())
            conn.execute(f"CREATE TABLE {schema}.{table} ({columns_sql})")
            if df.empty:
                return
        where_clause = " AND ".join(f"{col} = '{val}'" for col, val in match.items())
        conn.execute(f"DELETE FROM {schema}.{table} WHERE {where_clause}")
        conn.register("_incoming_df", df)
        conn.execute(f"INSERT INTO {schema}.{table} SELECT * FROM _incoming_df")
        conn.unregister("_incoming_df")
        return

    from snowflake.connector.pandas_tools import write_pandas

    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    table_exists = cursor.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {schema}").fetchall()
    if table_exists:
        where_clause = " AND ".join(f"{col.upper()} = '{val}'" for col, val in match.items())
        cursor.execute(f"DELETE FROM {schema}.{table} WHERE {where_clause}")
    write_pandas(
        conn=conn,
        df=df.rename(columns=str.upper),
        table_name=table.upper(),
        schema=schema.upper(),
        auto_create_table=True,
        quote_identifiers=True,
    )
