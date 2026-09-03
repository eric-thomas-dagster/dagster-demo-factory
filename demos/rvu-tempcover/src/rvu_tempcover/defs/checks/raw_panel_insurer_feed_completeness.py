"""Blocking completeness check on the panel insurer feed.

Maps to the brief's "no way to know if data output was good, only whether
the job ran" pain -- directly what Azure Data Factory can't tell Lisa's
team today. Every one of Tempcover's panel insurers should report every
day; a missing insurer means the panel's capacity picture is incomplete,
which is exactly the kind of gap `stg_panel_feed` (and, downstream,
`fct_bound_policies_daily`'s insurer reconciliation) shouldn't silently
build on top of.
"""

import dagster as dg

from rvu_tempcover.demo_data.warehouse import demo_duckdb_path

EXPECTED_PANEL_INSURER_COUNT = 7


@dg.asset_check(
    asset=dg.AssetKey("raw_panel_insurer_feed"),
    blocking=True,
    description=(
        f"Fails when fewer than {EXPECTED_PANEL_INSURER_COUNT} distinct panel "
        "insurers have reported in the feed -- blocks stg_panel_feed and "
        "everything reconciled against it from computing on an incomplete panel."
    ),
)
def raw_panel_insurer_feed_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        distinct_insurers = conn.execute(
            "select count(distinct insurer_id) from raw.panel_insurer_feed"
        ).fetchone()[0]
        row_count = conn.execute("select count(*) from raw.panel_insurer_feed").fetchone()[0]
    finally:
        conn.close()

    if distinct_insurers < EXPECTED_PANEL_INSURER_COUNT:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Only {distinct_insurers} of {EXPECTED_PANEL_INSURER_COUNT} expected panel "
                f"insurers reported across {row_count} feed rows. stg_panel_feed and every "
                "asset reconciled against it are blocked for this run."
            ),
            metadata={"distinct_insurers": distinct_insurers, "row_count": row_count},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {distinct_insurers} panel insurers reported, across {row_count} feed rows.",
        metadata={"distinct_insurers": distinct_insurers, "row_count": row_count},
    )
