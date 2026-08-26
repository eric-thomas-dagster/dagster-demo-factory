"""Blocking check: raw_dealer_floorplan_feed completeness -- the planted anomaly.

One partition (2026-08-22, midwest_dealers) ships with a floorplan advance
missing `loan_id`, per `demo_data/fabric_source_state.py`. This check fails
on it, and the blocking severity refuses to let the downstream Fabric
pipeline trigger fire for that partition -- exactly the money-shot scenario
in the brief. Recovery is a plain rematerialize once the mock source is
"corrected" (`make simulate-correction`), per `templates/demo_mode_pattern.py`.

Also doubles as the answer to the AE's "asset-level expected timing /
lateness visibility" ask -- an empty batch (nothing landed yet) fails this
same completeness check rather than a separate lateness check, since an
empty batch and a malformed batch are both "this partition isn't safe to
build on yet."
"""

import dagster as dg

from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["raw", "raw_dealer_floorplan_feed"]),
    blocking=True,
    description="Fails if the batch is empty (feed hasn't arrived) or any advance is missing loan_id.",
)
def raw_dealer_floorplan_feed_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    keys = context.partition_key.keys_by_dimension
    event_date, dealer_group = keys["date"], keys["dealer_group"]

    conn = connect_with_retry(demo_duckdb_path())
    try:
        total_rows, missing_loan_id = conn.execute(
            """
            select count(*), count(*) filter (where loan_id is null)
            from raw.dealer_floorplan_feed
            where advance_date = ? and dealer_group = ?
            """,
            [event_date, dealer_group],
        ).fetchone()
    finally:
        conn.close()

    if total_rows == 0:
        return dg.AssetCheckResult(
            passed=False,
            description=f"No floorplan advances landed yet for {dealer_group} on {event_date}.",
            metadata={"total_rows": 0, "missing_loan_id": 0, "event_date": event_date, "dealer_group": dealer_group},
        )
    if missing_loan_id > 0:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{missing_loan_id} of {total_rows} floorplan advances for {dealer_group} on {event_date} are "
                "missing loan_id -- the vendor file has a malformed record."
            ),
            metadata={
                "total_rows": total_rows,
                "missing_loan_id": missing_loan_id,
                "event_date": event_date,
                "dealer_group": dealer_group,
            },
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"All {total_rows} floorplan advances for {dealer_group} on {event_date} have loan_id.",
        metadata={
            "total_rows": total_rows,
            "missing_loan_id": 0,
            "event_date": event_date,
            "dealer_group": dealer_group,
        },
    )
