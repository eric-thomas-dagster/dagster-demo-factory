"""Non-blocking visibility check: did the vendor publish an advisory batch
for this day?

Maps to the brief's "trigger work only when needed" pain -- this is the
check that makes an empty day (nothing published yet, or a genuinely quiet
day) visible in the UI rather than silently indistinguishable from "still
waiting." Non-blocking because zero advisories on a quiet day is a normal
outcome, not a data-quality defect the way an incomplete telemetry batch is.
"""

import dagster as dg

from iso_new_england.demo_data.feed_state import advisory_has_arrived
from iso_new_england.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["raw", "external_feed_raw"]),
    description="Reports whether the vendor's advisory feed has published a batch for this day.",
)
def external_feed_arrival(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        row_count = conn.execute(
            "select count(*) from raw.external_feed_raw where event_date = ?",
            [event_date],
        ).fetchone()[0]
    finally:
        conn.close()

    arrived = advisory_has_arrived(event_date)
    return dg.AssetCheckResult(
        passed=arrived,
        description=(
            f"{row_count} advisories published for {event_date}."
            if arrived
            else f"Nothing published for {event_date} yet -- the sensor will trigger this "
            "asset automatically once the vendor's feed has a batch."
        ),
        metadata={"row_count": row_count, "event_date": event_date, "arrived": arrived},
    )
