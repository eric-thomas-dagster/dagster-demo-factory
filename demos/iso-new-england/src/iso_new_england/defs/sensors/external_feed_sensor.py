"""The sensor that replaces the "dumb fixed schedule" pain the brief names.

Polls the mock vendor feed's own arrival state (`demo_data/feed_state.py`) --
never Dagster instance state -- and fires a run the moment a day it hasn't
already requested shows up. Live-demo it with:

    python -m iso_new_england.demo_data.simulate_new_advisory

which marks "today" arrived in the mock feed; the sensor's next tick (every
30s) picks it up and only that partition -- not the whole day's batch of
other work -- recomputes.
"""

import json

import dagster as dg

from iso_new_england.demo_data.feed_state import arrived_dates

EXTERNAL_FEED_KEY = dg.AssetKey(["raw", "external_feed_raw"])


@dg.sensor(
    asset_selection=dg.AssetSelection.assets(EXTERNAL_FEED_KEY),
    minimum_interval_seconds=30,
    description=(
        "Fires the moment the vendor's operations-advisory feed publishes a "
        "new day's batch, instead of waiting on a fixed hourly/3-hourly schedule."
    ),
)
def external_feed_arrival_sensor(context: dg.SensorEvaluationContext):
    already_requested = set(json.loads(context.cursor)) if context.cursor else set()
    new_dates = [event_date for event_date in arrived_dates() if event_date not in already_requested]

    if not new_dates:
        return dg.SkipReason("No newly arrived advisory batches since the last check.")

    context.update_cursor(json.dumps(sorted(already_requested | set(new_dates))))
    return [
        dg.RunRequest(partition_key=event_date, run_key=f"external_feed_arrival_{event_date}")
        for event_date in new_dates
    ]
