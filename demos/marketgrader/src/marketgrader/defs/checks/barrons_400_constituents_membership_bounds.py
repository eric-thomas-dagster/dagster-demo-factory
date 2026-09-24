"""Warning-severity day-over-day membership-change bounds check.

Catches a scoring anomaly -- e.g. a bug suddenly flipping hundreds of
constituents -- without blocking a legitimate rebalance. Retained from the
prior (no-AE-notes) brief; the AE-notes rebuild kept it as the third of the
three named checks.

`barrons_400_constituents` is graph-first (Axis 2, stubbed -- see
`index_calculation/defs.yaml`), so there's no real constituent list to diff.
This evaluates a real, reproducible number instead of a hardcoded pass:
`constituent_count_for_date` (`demo_data/lakehouse.py`) is a deterministic
function of the partition date, same as the raw feeds, so the check is
doing genuine day-over-day comparison logic against synthetic-but-stable
data -- not faking a result.
"""

import datetime

import dagster as dg
from dagster import AssetCheckExecutionContext

from marketgrader.demo_data.lakehouse import constituent_count_for_date
from marketgrader.partitions import DAILY_PARTITIONS

# A same-day swing larger than this looks like a scoring bug, not a normal
# rebalance -- MarketGrader's real threshold isn't in the brief, so this is
# this build's own assumption (see README).
MAX_DAY_OVER_DAY_CHANGE = 5


@dg.asset_check(
    asset=dg.AssetKey("barrons_400_constituents"),
    blocking=False,
    description=(
        f"Warns when the constituent count moves more than {MAX_DAY_OVER_DAY_CHANGE} "
        "day-over-day -- catches a scoring anomaly without blocking a legitimate rebalance."
    ),
)
def barrons_400_constituents_membership_bounds(context: AssetCheckExecutionContext) -> dg.AssetCheckResult:
    today = context.partition_key
    today_count = constituent_count_for_date(today)

    prev_date = (datetime.date.fromisoformat(today) - datetime.timedelta(days=1)).isoformat()
    if not DAILY_PARTITIONS.has_partition_key(prev_date):
        return dg.AssetCheckResult(
            passed=True,
            description=f"No prior partition to compare ({today} is the first day in this window).",
            metadata={"constituent_count": today_count},
        )

    prev_count = constituent_count_for_date(prev_date)
    delta = abs(today_count - prev_count)
    passed = delta <= MAX_DAY_OVER_DAY_CHANGE
    metadata = {
        "constituent_count": today_count,
        "prior_day_constituent_count": prev_count,
        "day_over_day_change": delta,
        "max_expected_change": MAX_DAY_OVER_DAY_CHANGE,
    }
    return dg.AssetCheckResult(
        passed=passed,
        description=(
            f"{today}: {today_count} constituents ({'+' if today_count >= prev_count else ''}"
            f"{today_count - prev_count} vs {prev_date}), within the expected "
            f"+/-{MAX_DAY_OVER_DAY_CHANGE} range."
            if passed
            else (
                f"{today}: {today_count} constituents moved {delta} from {prev_date}'s "
                f"{prev_count} -- beyond the expected +/-{MAX_DAY_OVER_DAY_CHANGE}."
            )
        ),
        metadata=metadata,
    )
