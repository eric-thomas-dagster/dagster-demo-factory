"""The fixed schedule ISO-NE runs today: this is deliberately the "dumb
schedule regardless of whether work is needed" pain from the brief, kept
running as-is so `external_feed_raw`'s sensor is a visible contrast, not an
abstract pitch. Runs at 2am ET, well inside the historical legacy Oracle
batch window ISO-NE described.
"""

import dagster as dg

legacy_oracle_extract_job = dg.define_asset_job(
    name="legacy_oracle_extract_job",
    selection=dg.AssetSelection.assets(dg.AssetKey(["raw", "legacy_oracle_extract"])),
)

legacy_oracle_extract_schedule = dg.build_schedule_from_partitioned_job(
    legacy_oracle_extract_job,
    hour_of_day=2,
    minute_of_hour=0,
    name="legacy_oracle_extract_schedule",
    description=(
        "Runs the legacy Oracle extract every night at 2am ET, on a fixed "
        "interval regardless of whether new data is actually ready -- the "
        "current-state pattern `external_feed_arrival_sensor` replaces."
    ),
)
