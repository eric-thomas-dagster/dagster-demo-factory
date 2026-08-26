"""Warning-severity arrival-time check on the dealer floorplan feed.

Maps directly to the brief's stated interest in "asset-level expected
timing / lateness visibility and downstream impact awareness." Every
regional dealer group is expected to report every one of its dealers each
day; a region reporting fewer dealers than its roster is the lateness
signal worth a look, without being severe enough to block `dim_dealer`'s
rollup the way a corrupted record would.
"""

import dagster as dg

from stellantis_financial_services.components.partitions import DEALER_GROUPS
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path

_DEALERS_PER_GROUP = 10


@dg.asset_check(
    asset=dg.AssetKey("raw_dealer_floorplan_feed"),
    blocking=False,
    description="Warns when a dealer group's floorplan feed reports fewer dealers than its roster -- the lateness/incomplete-arrival signal.",
)
def dealer_floorplan_feed_lateness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    partition_key = context.partition_key
    report_date = partition_key.keys_by_dimension["date"]
    dealer_group = partition_key.keys_by_dimension["dealer_group"]

    conn = connect_with_retry(demo_duckdb_path())
    try:
        reported = conn.execute(
            "select count(*) from raw.dealer_floorplan_feed where report_date = ? and dealer_group = ?",
            [report_date, dealer_group],
        ).fetchone()[0]
    finally:
        conn.close()

    if dealer_group not in DEALER_GROUPS:
        return dg.AssetCheckResult(passed=True, description=f"Unrecognized dealer_group {dealer_group!r}, skipping.")

    if reported < _DEALERS_PER_GROUP:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=(
                f"{dealer_group} reported {reported} of {_DEALERS_PER_GROUP} dealers for {report_date} -- "
                "worth confirming whether the remaining dealers' floorplan reports are simply late."
            ),
            metadata={"reported_count": reported, "expected_count": _DEALERS_PER_GROUP, "report_date": report_date, "dealer_group": dealer_group},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"{dealer_group} reported all {reported} dealers on time for {report_date}.",
        metadata={"reported_count": reported, "expected_count": _DEALERS_PER_GROUP, "report_date": report_date, "dealer_group": dealer_group},
    )
