"""Live-demo helper: simulate a new day's advisory landing in the mock feed.

An operation on the mock source system, run OUTSIDE Dagster -- never a
Dagster asset, job, or control node. Run this while `dg dev` is up (with the
`external_feed_arrival_sensor` toggled on) to watch the sensor pick up the
new day and fire a run for `external_feed_raw`, without touching anything
inside Dagster.

Usage:
    python -m iso_new_england.demo_data.simulate_new_advisory [YYYY-MM-DD]

Defaults to today (America/New_York) if no date is given.
"""

from __future__ import annotations

import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from iso_new_england.demo_data.feed_state import mark_advisory_arrived


def main() -> int:
    if len(sys.argv) > 1:
        event_date = sys.argv[1]
    else:
        event_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    mark_advisory_arrived(event_date)
    print(f"Marked {event_date} as arrived in the mock advisory feed.")
    print("The external_feed_arrival_sensor will pick it up on its next tick.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
