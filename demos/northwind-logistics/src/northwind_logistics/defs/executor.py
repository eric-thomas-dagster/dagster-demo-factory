"""Run every asset in a single process.

The demo warehouse is a local DuckDB file, which only allows one writer
connection at a time (see https://duckdb.org/docs/stable/connect/concurrency).
Dagster's default multiprocess executor runs each step in its own subprocess,
which deadlocks the moment two DuckDB-writing steps overlap. In real mode,
Snowflake has no such constraint -- this is purely a demo-mode concession, not
something that would apply against a live warehouse.
"""

import dagster as dg


@dg.definitions
def executor_defs() -> dg.Definitions:
    return dg.Definitions(executor=dg.in_process_executor)
