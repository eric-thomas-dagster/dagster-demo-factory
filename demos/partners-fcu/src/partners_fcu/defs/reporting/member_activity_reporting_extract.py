"""`member_activity_reporting_extract` -- a plain `@asset`, not a component.

No BI tool is named anywhere in the AE notes or public research for
Partners FCU (see brief: "Data domains" / "Current stack"), so per house
rules this asset gets **no kind badge** rather than a guessed one (no
Power BI/Tableau/Looker invention) -- there is no external system here to
route through a registry component; this is the one hand-written,
Axis-2-only asset in the graph (a rollup/export with no integration
surface of its own), which is exactly the case CLAUDE.md carves out for a
plain `@asset` rather than a component.

Fidelity is graph-first per the brief: the row count read below is real
(from `fct_daily_member_activity`, materialized for real by dbt); nothing
downstream of this asset is built (no BI tool to actually export to), so
this asset's own body just reports the shape of the extract it would
produce.
"""

import dagster as dg

from partners_fcu.demo_data.warehouse import demo_duckdb_path


@dg.asset(
    key="member_activity_reporting_extract",
    deps=[dg.AssetDep(dg.AssetKey(["marts", "fct_daily_member_activity"]))],
    group_name="reporting",
    owners=["team:partners-fcu-data-platform"],
    description=(
        "Daily member-activity extract for downstream BI consumption -- no BI "
        "tool is named in the brief, so this asset has no kind badge and "
        "nothing is built downstream of it."
    ),
    metadata={
        "owner": "Partners FCU Data Platform",
        "owner_team": "team:partners-fcu-data-platform",
        "tier": "tier_2",
        "domain": "member_activity",
        "business_impact": (
            "Sits in the same lineage graph as the dbt pipeline it's derived from -- "
            "not a side export nobody can trace back to its source."
        ),
    },
)
def member_activity_reporting_extract(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        activity_row_count = conn.execute(
            "select count(*) from main_marts.fct_daily_member_activity"
        ).fetchone()[0]
    finally:
        conn.close()

    context.log.info(
        f"member_activity_reporting_extract: {activity_row_count} daily activity "
        "rows available for extract."
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": activity_row_count,
            "source": dg.MetadataValue.text(
                "graph-first -- no BI tool named in the brief, so no export is "
                "actually performed; this asset reports the shape of the extract "
                "it would produce"
            ),
        }
    )
