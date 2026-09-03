"""`braze_customer_segment_export` -- a plain, demo-mode-mocked `@asset`.

Not a component. Three distinct registry searches turned up zero hits for
Braze (`dagster-component search "braze" --json`,
`"braze customer segment export" --json`,
`"customer engagement marketing activation" --json`) and there is no
native `dagster-braze` package -- see
`component-feedback/2026-09-03-braze-export.md` for the full search
record. Per the brief's explicit fallback ("a plain demo-mode-mocked
`@asset` ... if nothing turns up, that's a `component-feedback/` entry")
and `CLAUDE.md`'s rung 4, a hand-written asset is the right size here: this
is one asset syncing to one system, not a workspace of many objects with a
reusable enumerate/execute/observe shape a component would earn its keep
wrapping.

Real mode would POST the daily segment export to Braze's
`/users/track` (or a custom-attribute / catalog) endpoint using
`BRAZE_API_KEY` / `BRAZE_REST_ENDPOINT`. `DEMO_MODE` fakes only that
outermost network call, per `templates/demo_mode_pattern.py` -- the row
count read below is real (from `fct_quotes_daily`, materialized for real
by dbt), only the Braze POST itself is simulated.
"""

import dagster as dg

from rvu_tempcover.demo_data.warehouse import demo_duckdb_path

DEMO_MODE = True


@dg.asset(
    key="braze_customer_segment_export",
    deps=[dg.AssetDep(dg.AssetKey(["marts", "fct_quotes_daily"]))],
    group_name="activation",
    kinds={"braze"},
    owners=["team:rvu-data-platform"],
    description=(
        "Customer segments derived from daily quote behaviour, exported to Braze for "
        "activation campaigns (e.g. re-engaging abandoned quotes)."
    ),
    metadata={
        "owner": "RVU Data Platform",
        "owner_team": "team:rvu-data-platform",
        "tier": "tier_2",
        "domain": "activation",
        "business_impact": (
            "Sits in the same lineage graph as the quoting pipeline it's derived from -- "
            "not a side integration nobody can trace."
        ),
    },
)
def braze_customer_segment_export(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        quote_count = conn.execute("select count(*) from main_marts.fct_quotes_daily").fetchone()[0]
    finally:
        conn.close()

    if not DEMO_MODE:
        # Real mode: POST the segment export to Braze's REST API using
        # BRAZE_API_KEY / BRAZE_REST_ENDPOINT here.
        raise NotImplementedError("Set BRAZE_API_KEY/BRAZE_REST_ENDPOINT and implement the real POST.")

    context.log.info(f"braze_customer_segment_export: exporting segments derived from {quote_count} quote rows.")
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": quote_count,
            "braze/export_status": "Completed",
            "source": dg.MetadataValue.text(
                "simulated -- set DEMO_MODE = False and implement the real Braze POST to export live"
            ),
        }
    )
