"""Mock state for the external operations-advisory feed `external_feed_raw` reads.

This models the *vendor's own system*, not Dagster: which days it has
published an advisory batch for. `external_feed_arrival_sensor` (see
`defs/sensors/`) polls this state and fires a `RunRequest` the moment a new
day shows up -- the "trigger only when needed" story from the brief, told
with a real Dagster sensor instead of a dumb fixed-interval schedule.

The default demo window (`DEFAULT_WINDOW_START..DEFAULT_WINDOW_END`) is
auto-seeded as "already arrived" the first time this module is read, so a
fresh clone is green with zero setup. Demonstrating the sensor live means
running `demo_data/simulate_new_advisory.py`, which marks one more day
arrived -- an operation on the mock source, never on Dagster.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

DEFAULT_WINDOW_START = "2026-08-11"
DEFAULT_WINDOW_END = "2026-08-24"

_STATE_PATH = Path(__file__).parent / "_advisory_feed_state.json"


def _default_window() -> list[str]:
    return list(pd.date_range(DEFAULT_WINDOW_START, DEFAULT_WINDOW_END, freq="D").strftime("%Y-%m-%d"))


def _read_state() -> dict:
    if not _STATE_PATH.exists():
        state = {"arrived": _default_window()}
        _STATE_PATH.write_text(json.dumps(state, indent=2))
        return state
    try:
        return json.loads(_STATE_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {"arrived": _default_window()}


def advisory_has_arrived(event_date: str) -> bool:
    return event_date in _read_state().get("arrived", [])


def mark_advisory_arrived(event_date: str) -> None:
    state = _read_state()
    arrived = set(state.get("arrived", []))
    arrived.add(event_date)
    state["arrived"] = sorted(arrived)
    _STATE_PATH.write_text(json.dumps(state, indent=2))


def arrived_dates() -> list[str]:
    return sorted(_read_state().get("arrived", []))


def reset_source_state() -> None:
    """The only way the mock feed resets -- an operation on the source, never on Dagster."""
    _STATE_PATH.unlink(missing_ok=True)
