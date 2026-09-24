"""Blocking arrival/completeness check on the daily price feed.

Direct answer to Jim's discovery question #1 -- "when a price feed... is
late, how do you find out today, and what does a late or wrong index cost
you with clients?" A day with too few priced symbols means the feed landed
incomplete; `barrons_400_constituents` refuses to compute on top of a
partial day, the way today's SQL Agent/SSIS chain wouldn't stop it from
doing silently.

Reads back the real rows `LakehouseTableAssetsComponent` just wrote through
the (demo-mode) Iceberg IO manager, for this partition's date specifically
-- not a hardcoded pass. No registry component covers a declarative
completeness check against an arbitrary Iceberg table partition; asset-check
assertion logic is business logic, one of the justified `.py` files in
`defs/checks/` (see README).
"""

import datetime

import dagster as dg
from dagster import AssetCheckExecutionContext
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

from marketgrader.demo_data.lakehouse import UNIVERSE, demo_iceberg_catalog_properties

# Universe is 60 symbols; a floor of 50 leaves slack for a partial-but-still-
# real trading day without masking a genuinely incomplete feed.
MINIMUM_EXPECTED_ROWS = 50


@dg.asset_check(
    asset=dg.AssetKey("raw_price_feed"),
    blocking=True,
    description=(
        f"Fails when fewer than {MINIMUM_EXPECTED_ROWS} of the {len(UNIVERSE)}-symbol "
        "POC universe priced for the day -- blocks barrons_400_constituents from "
        "computing on an incomplete price feed."
    ),
)
def raw_price_feed_arrival(context: AssetCheckExecutionContext) -> dg.AssetCheckResult:
    d = datetime.date.fromisoformat(context.partition_key)
    catalog = load_catalog("marketgrader_lake", **demo_iceberg_catalog_properties())
    table = catalog.load_table("raw.raw_price_feed")
    row_count = table.scan(row_filter=EqualTo("date", d)).to_arrow().num_rows

    if row_count < MINIMUM_EXPECTED_ROWS:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Only {row_count} symbols priced for {context.partition_key} against a "
                f"floor of {MINIMUM_EXPECTED_ROWS} -- the daily price feed looks incomplete. "
                "barrons_400_constituents is blocked for this run."
            ),
            metadata={"row_count": row_count, "minimum_expected_rows": MINIMUM_EXPECTED_ROWS},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"{row_count} symbols priced for {context.partition_key}, above the floor of {MINIMUM_EXPECTED_ROWS}.",
        metadata={"row_count": row_count, "minimum_expected_rows": MINIMUM_EXPECTED_ROWS},
    )
