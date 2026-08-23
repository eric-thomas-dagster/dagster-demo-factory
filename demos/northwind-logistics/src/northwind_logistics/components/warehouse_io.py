"""Shared DuckDB write helpers for the demo-mode ingestion components.

Every ingestion component in this project is demo-only (see each component's
docstring for why a real extraction path is out of scope for this build) and
writes synthetic rows through the same `warehouse` resource in
`defs/resources.py`. Centralizing the DDL/DML here means a schema change only
happens in one place.
"""

import pandas as pd

_SQL_TYPE_BY_DTYPE = {
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "bool": "BOOLEAN",
    "object": "VARCHAR",
}


def _sql_type_for(dtype: object) -> str:
    return _SQL_TYPE_BY_DTYPE.get(str(dtype), "VARCHAR")


def _create_table_if_missing(conn: object, schema: str, table: str, frame: pd.DataFrame) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    columns_ddl = ", ".join(f'"{col}" {_sql_type_for(dtype)}' for col, dtype in frame.dtypes.items())
    conn.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns_ddl})")


def _insert_rows(conn: object, schema: str, table: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    placeholders = ", ".join(["?"] * len(frame.columns))
    insert_sql = f"INSERT INTO {schema}.{table} VALUES ({placeholders})"
    conn.executemany(insert_sql, frame.itertuples(index=False, name=None))


def replace_partition_rows(
    conn: object,
    schema: str,
    table: str,
    frame: pd.DataFrame,
    partition_column: str,
    partition_value: str,
) -> None:
    """Create `schema.table` if needed, then replace all rows for one partition value."""
    _create_table_if_missing(conn, schema, table, frame)
    conn.execute(f'DELETE FROM {schema}.{table} WHERE "{partition_column}" = ?', [partition_value])
    _insert_rows(conn, schema, table, frame)


def replace_all_rows(conn: object, schema: str, table: str, frame: pd.DataFrame) -> None:
    """Create `schema.table` if needed, then replace its entire contents (unpartitioned sources)."""
    _create_table_if_missing(conn, schema, table, frame)
    conn.execute(f"DELETE FROM {schema}.{table}")
    _insert_rows(conn, schema, table, frame)
