"""The blocking check: refuse to run dbt on a partial telemetry batch.

Blocking so every downstream dbt model refuses to compute over an
incomplete day rather than reproducing ISO-NE's current situation -- a
number published with confidence over data nobody checked -- with nicer
styling. Maps to the brief's "operational complexity around views vs.
materialized tables": don't materialize downstream on bad input.
"""

import dagster as dg

from iso_new_england.demo_data.generators import REPORTING_POINTS
from iso_new_england.demo_data.warehouse import connect_with_retry, demo_duckdb_path

_EXPECTED_INTERVALS_PER_DAY = len(REPORTING_POINTS) * 24
_MIN_ACCEPTABLE_INTERVALS = int(_EXPECTED_INTERVALS_PER_DAY * 0.9)


@dg.asset_check(
    asset=dg.AssetKey(["staged", "staged_readings"]),
    blocking=True,
    description="Fails when the day's landed interval-reading batch is materially incomplete.",
)
def staged_readings_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        row_count = conn.execute(
            "select count(*) from staged.staged_readings where event_date = ?",
            [event_date],
        ).fetchone()[0]
    finally:
        conn.close()

    if row_count < _MIN_ACCEPTABLE_INTERVALS:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Only {row_count} interval readings landed for {event_date}, expected "
                f"at least {_MIN_ACCEPTABLE_INTERVALS} of {_EXPECTED_INTERVALS_PER_DAY}. "
                "Downstream dbt models are blocked for this partition, not computed on a "
                "partial batch."
            ),
            metadata={"row_count": row_count, "expected_minimum": _MIN_ACCEPTABLE_INTERVALS, "event_date": event_date},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"{row_count} interval readings landed for {event_date} (expected {_EXPECTED_INTERVALS_PER_DAY}).",
        metadata={"row_count": row_count, "event_date": event_date},
    )
