"""Demo-mode stand-in for "Fivetran has finished loading the warehouse".

Real Fivetran lands the replicated tables in Snowflake by itself; there is no
Dagster code on that path. With no Fivetran account or Snowflake here, this
writes the equivalent rows into the local DuckDB warehouse -- delete+insert
per `(tenant, date)` slice, so re-landing a slice is idempotent.
"""

from skygen.demo_data.tenant_source import COLUMNS, TABLES, rows_for
from skygen.demo_data.warehouse import connect

LANDING_SCHEMA = "client_landing"


def land_slice(tenant: str, day: str, tables: tuple[str, ...] = TABLES) -> dict[str, int]:
    """Land `(tenant, day)` for each table; return rows landed per table."""
    landed: dict[str, int] = {}
    with connect() as conn:
        conn.execute(f"create schema if not exists {LANDING_SCHEMA}")
        for table in tables:
            columns = ", ".join(f"{name} {dtype}" for name, dtype in COLUMNS[table].items())
            conn.execute(f"create table if not exists {LANDING_SCHEMA}.{table} ({columns})")
            conn.execute(
                f"delete from {LANDING_SCHEMA}.{table} where tenant_id = ? and report_date = ?",
                [tenant, day],
            )
            rows = rows_for(table, tenant, day)
            placeholders = ", ".join("?" for _ in COLUMNS[table])
            conn.executemany(f"insert into {LANDING_SCHEMA}.{table} values ({placeholders})", rows)
            landed[table] = len(rows)
    return landed
