"""Mock state for the dealer floorplan vendor feed -- models the *vendor's own
system*, not Dagster.

One specific (feed, date) combination starts out uncorrected: the dealer's
floorplan file for that day is missing the VIN on one advance record, exactly
as it would arrive from a real vendor with a form-parsing bug on their end.
Every other feed and date is clean. `raw_dealer_floorplan_feed` reads this
state and returns whatever the vendor's system currently has -- the asset
itself never changes; only what upstream has changed.

Demonstrating the recovery means running
`python -m stellantis_financial_services.demo_data.simulate_corrected_feed`,
which marks the file "resent, corrected" -- an operation on the mock source,
never on Dagster. Resetting the demo (`make reset-demo`) reverts it.
"""

from __future__ import annotations

import json
from pathlib import Path

ANOMALY_FEED = "dealer_floorplan"
ANOMALY_DATE = "2026-08-20"

_STATE_PATH = Path(__file__).parent / "_vendor_feed_state.json"


def _read_state() -> dict:
    if not _STATE_PATH.exists():
        return {}
    try:
        return json.loads(_STATE_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def is_corrected(feed: str, event_date: str) -> bool:
    """Whether the vendor's file for this (feed, date) is the clean version.

    Every combination except the one planted anomaly is always clean.
    """
    if feed != ANOMALY_FEED or event_date != ANOMALY_DATE:
        return True
    return bool(_read_state().get("corrected", False))


def mark_corrected() -> None:
    """Simulates the dealer resending a corrected file. Never called by an asset."""
    _STATE_PATH.write_text(json.dumps({"corrected": True}, indent=2))


def reset_source_state() -> None:
    """The only way the mock feed resets -- an operation on the source, never on Dagster."""
    _STATE_PATH.unlink(missing_ok=True)
