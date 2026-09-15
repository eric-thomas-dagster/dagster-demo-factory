"""`raw_bookings_feed` -- an external/observable source asset, not a
materialized one.

Standing in for "the dbt model that ingests data" from Noel's own project
(brief: Use case). Modelled as external per CLAUDE.md's "orchestrating
existing workloads" guidance ("observable source assets to report
freshness on data Dagster doesn't materialize") rather than as a component-
built ingestion asset like demos/partners-fcu's SQL-Server layer, because
there is no ingestion *tool* named in the brief to route through a
component -- this is Bokadirekt's own booking platform, whose data simply
exists upstream. The underlying `raw.raw_bookings` table is seeded once by
`demo_data/seed.py`, outside Dagster's own materialization narrative
(house rule: "Dagster reads it; Dagster never writes it as part of the
demo narrative").

No registry component covers "observe an arbitrary DuckDB/BigQuery table
for freshness" as a general primitive -- `@dg.observable_source_asset` is
core Dagster, not an integration surface, so the component escalation
ladder doesn't apply here (same reasoning as the plain `@asset` used for
`specialist_utilization_daily`).

A polling sensor re-observes on an interval so runs Dagster didn't trigger
still register -- the same "observe, don't just execute" expectation the
brief's workspace-component convention holds external-job components to,
scaled down to a single external asset.
"""

import dagster as dg

from bokadirekt.demo_data.warehouse import demo_duckdb_path


@dg.observable_source_asset(
    key="raw_bookings_feed",
    group_name="ingestion",
    kinds={"bigquery"},
    owners=["team:bokadirekt-data"],
    description=(
        "Bokadirekt's own booking-platform export -- the feed Noel's "
        "'model that ingests data' reads from. Dagster observes freshness "
        "here rather than materializing it; the booking platform owns "
        "this data."
    ),
    metadata={
        "owner": "Bokadirekt Data",
        "owner_team": "team:bokadirekt-data",
        "tier": "tier_1",
        "domain": "bookings",
        "deployment_mode": "self-serve trial (Dagster Starter today; this demo shows the next tier)",
        "integration_pattern": "coexistence",
        "business_impact": "Everything in this demo's staging and entity layers reads from this feed.",
    },
)
def raw_bookings_feed(context: dg.AssetExecutionContext) -> dg.DataVersion:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        row_count, latest_date = conn.execute(
            "select count(*), max(booking_date) from raw.raw_bookings"
        ).fetchone()
    finally:
        conn.close()

    context.log.info(f"raw_bookings_feed: {row_count} rows observed, latest booking_date={latest_date}")
    context.add_output_metadata(
        {
            "dagster/row_count": row_count,
            "latest_booking_date": str(latest_date),
            "source": dg.MetadataValue.text(
                "simulated -- set demo_mode off and point profiles.yml's 'live' "
                "target at BigQuery to observe the real feed"
            ),
        }
    )
    return dg.DataVersion(f"{row_count}:{latest_date}")


raw_bookings_feed_observation_job = dg.define_asset_job(
    name="raw_bookings_feed_observation_job",
    selection=dg.AssetSelection.keys(dg.AssetKey("raw_bookings_feed")),
)


@dg.sensor(
    job=raw_bookings_feed_observation_job,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Polls the booking platform's export on an interval so bookings "
        "that land outside any Dagster-triggered run still produce an "
        "AssetObservation -- answers 'what if it wasn't Dagster that "
        "updated this.'"
    ),
)
def raw_bookings_feed_observation_sensor(context: dg.SensorEvaluationContext) -> dg.RunRequest:
    return dg.RunRequest()
