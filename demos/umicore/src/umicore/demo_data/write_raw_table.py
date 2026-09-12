"""Writes a stub DataFrame into the shared demo DuckDB warehouse.

Shared by the generic `WarehouseTableAssetsComponent` (recycling,
specialty_materials -- no ingestion tool named in the brief) and the
demo-mode Databricks workspace component (battery_materials, catalysis/AC --
standing in for the real PySpark/Databricks job that would land the table).
Both write through this one function so `raw.<table>` always lands the same
way regardless of which component produced it -- per-partition DELETE+INSERT
so re-materializing a partition is idempotent, not an append.
"""

import duckdb
import pandas as pd

from umicore.demo_data.warehouse import demo_duckdb_path


def write_raw_table(table_name: str, frame: pd.DataFrame, partition_column: str, partition_value: str) -> int:
    schema, table = table_name.split(".", 1)
    conn = duckdb.connect(demo_duckdb_path())
    try:
        conn.execute(f"create schema if not exists {schema}")
        conn.execute(f"create table if not exists {table_name} as select * from frame limit 0")
        conn.execute(
            f"delete from {table_name} where {partition_column} = ?",
            [partition_value],
        )
        conn.execute(f"insert into {table_name} select * from frame")
        return conn.execute(f"select count(*) from {table_name} where {partition_column} = ?", [partition_value]).fetchone()[0]
    finally:
        conn.close()
