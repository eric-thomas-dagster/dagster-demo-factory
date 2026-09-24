"""Blocking arrival/completeness check on the daily corporate-action feed.

Same discovery-question-#1 reasoning as `raw_price_feed_arrival`, but this
feed is sparse by design -- most days carry zero-to-a-handful of corporate
actions across the 60-symbol universe, so "row count above a floor" isn't a
meaningful completeness signal here (zero actions is a legitimate, complete
result). Instead this reconciles what actually landed in the Iceberg table
against what the source system's own record says should be there for that
date -- the real-world shape of a corporate-action completeness check (did
the write match the source, not "is the count non-zero"). A genuinely
dropped or partial write shows up as a mismatch; a quiet day does not.

Reads back the real rows `LakehouseTableAssetsComponent` wrote through the
(demo-mode) Iceberg IO manager. No registry component covers this; see
`raw_price_feed_arrival.py`'s docstring for why it's one of the justified
`.py` files in `defs/checks/`.
"""

import datetime

import dagster as dg
from dagster import AssetCheckExecutionContext
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

from marketgrader.demo_data.lakehouse import demo_iceberg_catalog_properties, generate_corporate_actions


@dg.asset_check(
    asset=dg.AssetKey("raw_corporate_actions"),
    blocking=True,
    description=(
        "Fails when the landed row count for the day doesn't match the source "
        "system's own record count -- blocks barrons_400_constituents from "
        "computing on a partial or dropped corporate-action write."
    ),
)
def raw_corporate_actions_arrival(context: AssetCheckExecutionContext) -> dg.AssetCheckResult:
    d = datetime.date.fromisoformat(context.partition_key)
    expected_count = generate_corporate_actions(context.partition_key).num_rows

    catalog = load_catalog("marketgrader_lake", **demo_iceberg_catalog_properties())
    table = catalog.load_table("raw.raw_corporate_actions")
    landed_count = table.scan(row_filter=EqualTo("date", d)).to_arrow().num_rows

    passed = landed_count == expected_count
    metadata = {"landed_count": landed_count, "expected_count": expected_count}
    if not passed:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"{landed_count} corporate actions landed for {context.partition_key} against "
                f"{expected_count} expected from the source -- write looks partial. "
                "barrons_400_constituents is blocked for this run."
            ),
            metadata=metadata,
        )
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"{landed_count} corporate actions landed for {context.partition_key}, matching the "
            "source system's own record count."
        ),
        metadata=metadata,
    )
