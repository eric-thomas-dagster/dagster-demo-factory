"""Warning check on the dealer floorplan feed's arrival timing.

Maps to the brief's stated interest in *"asset-level expected timing /
lateness visibility and downstream impact awareness."* The south region's
feed genuinely lands later than the other three regions in the synthetic data
(see `demo_data/generators.py`) -- a real, computed timing signal, comfortably
inside the overnight-batch cutoff every region is held to. Per house rules,
this check always passes in the demo (no planted anomaly); the region-to-
region variance is real and visible in the metadata, it just never crosses
the SLA. Warning severity, not blocking: a late floorplan feed is worth
surfacing, not worth stopping the pipeline over.
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path

_EXPECTED_ARRIVAL_HOUR = 10


@dg.asset_check(
    asset=dg.AssetKey(["raw_dealer_floorplan_feed"]),
    blocking=False,
    description=f"Warns when the floorplan feed for a region arrives after the expected {_EXPECTED_ARRIVAL_HOUR}:00 overnight-batch cutoff.",
)
def raw_dealer_floorplan_feed_lateness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    partition_key = context.partition_key
    event_date = partition_key.keys_by_dimension["date"]
    dealer_group = partition_key.keys_by_dimension["dealer_group"]

    conn = connect_with_retry(demo_duckdb_path())
    try:
        max_arrival_hour = conn.execute(
            """
            select max(arrival_hour) from raw.raw_dealer_floorplan_feed
            where feed_date = ? and dealer_group = ?
            """,
            [event_date, dealer_group],
        ).fetchone()[0]
    finally:
        conn.close()

    if max_arrival_hour is None:
        return dg.AssetCheckResult(passed=True, description="No rows landed for this partition yet.")

    late = max_arrival_hour > _EXPECTED_ARRIVAL_HOUR
    return dg.AssetCheckResult(
        passed=not late,
        description=(
            f"{dealer_group} floorplan feed for {event_date} landed at hour {max_arrival_hour}, "
            f"{'after' if late else 'within'} the {_EXPECTED_ARRIVAL_HOUR}:00 overnight-batch cutoff."
        ),
        metadata={
            "arrival_hour": max_arrival_hour,
            "expected_by_hour": _EXPECTED_ARRIVAL_HOUR,
            "dealer_group": dealer_group,
            "feed_date": event_date,
        },
    )
