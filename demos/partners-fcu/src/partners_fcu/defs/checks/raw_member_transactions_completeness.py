"""Blocking completeness check on the daily member transaction feed.

Direct reversal of the brief's named pain -- "always building workarounds
to get jobs to run in the right order and have dependencies running
properly." A day with implausibly few transactions means the SQL Server
feed landed incomplete; the dbt layer (and everything reconciled against
it) shouldn't silently compute on top of a partial day, the way a
hand-sequenced job would today.

No registry component covers a declarative completeness check against an
arbitrary DuckDB query -- asset-check assertion logic is business logic,
one of the two justified `.py` files in `defs/checks/` (see README).
"""

import dagster as dg

from partners_fcu.demo_data.warehouse import demo_duckdb_path

MINIMUM_EXPECTED_ROWS = 100


@dg.asset_check(
    asset=dg.AssetKey("raw_member_transactions"),
    blocking=True,
    description=(
        f"Fails when fewer than {MINIMUM_EXPECTED_ROWS} member transactions "
        "landed for the day -- blocks stg_member_transactions and everything "
        "downstream from computing on an incomplete daily feed."
    ),
)
def raw_member_transactions_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        row_count = conn.execute("select count(*) from raw.raw_member_transactions").fetchone()[0]
    finally:
        conn.close()

    if row_count < MINIMUM_EXPECTED_ROWS:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Only {row_count} member transactions on file against a floor of "
                f"{MINIMUM_EXPECTED_ROWS} -- the daily feed looks incomplete. "
                "stg_member_transactions and every asset downstream of it are "
                "blocked for this run."
            ),
            metadata={"row_count": row_count, "minimum_expected_rows": MINIMUM_EXPECTED_ROWS},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"{row_count} member transactions on file, above the floor of {MINIMUM_EXPECTED_ROWS}.",
        metadata={"row_count": row_count, "minimum_expected_rows": MINIMUM_EXPECTED_ROWS},
    )
