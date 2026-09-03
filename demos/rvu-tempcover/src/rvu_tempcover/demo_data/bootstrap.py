"""Loads the demo's fixture data into the local DuckDB warehouse's `raw`
schema, idempotently, at Definitions-load time.

This stands in for what Fivetran already having synced RVU's source systems
into the warehouse would look like -- it is *not* a Dagster asset and has no
lineage node, because in production Fivetran itself puts this data there,
not Dagster (see CLAUDE.md: "Assets are idempotent -- the source changes,
not the asset" / "Mock source state lives outside Dagster"). The four
`raw_*` Dagster assets in `defs/ingestion/defs.yaml` are graph-first,
pass-bodied lineage nodes representing those same Fivetran syncs; this
module is what makes the tables they represent actually queryable so the
real dbt project downstream has something to transform and test.

Runs on every process that imports `rvu_tempcover.definitions` -- a fresh
clone, a fresh Dagster+ Serverless container, or a local `dg dev` all
re-populate the same deterministic fixture rows from the static CSVs
checked into `demo_data/fixtures/`. `CREATE OR REPLACE TABLE ... AS SELECT`
is naturally idempotent, so re-running this on an already-populated file
(e.g. a second `dg dev` reload) is a cheap no-op in effect, not an error.
"""

from pathlib import Path

import duckdb

from rvu_tempcover.demo_data.warehouse import demo_duckdb_path

_FIXTURES_DIR = Path(__file__).parent / "fixtures"

_TABLES = ["quote_requests", "bound_policies", "panel_insurer_feed", "partner_broker_feed"]


def ensure_raw_fixtures_loaded() -> None:
    conn = duckdb.connect(demo_duckdb_path())
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for table in _TABLES:
            csv_path = _FIXTURES_DIR / f"{table}.csv"
            conn.execute(
                f"CREATE OR REPLACE TABLE raw.{table} AS "
                f"SELECT * FROM read_csv_auto('{csv_path.as_posix()}', header=true)"
            )
    finally:
        conn.close()
