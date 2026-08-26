"""Mock state for the vendor dealer-floorplan feed `raw_dealer_floorplan_feed` reads.

This models the *dealer's own file drop*, not Dagster: whether the corrected
version of one flagged (date, dealer_group) batch has arrived yet. On the
first read, that one batch comes back with one malformed row (a floorplan
advance missing its `loan_id`) -- exactly what a real vendor file with a bad
record looks like. `raw_dealer_floorplan_feed_completeness` (see
`defs/checks/`) fails on it, and the blocking check refuses to let the
downstream Fabric pipeline trigger fire for that partition.

Recovery is: the dealer resends a corrected file (`mark_corrected` below),
and a plain rematerialize of just that one partition succeeds -- no heal
step, no reset object, per `templates/demo_mode_pattern.py`. This file is the
*only* thing that changes; the asset itself is unchanged and idempotent.

The flagged partition is pre-seeded as "not yet corrected" so a fresh clone
reproduces the anomaly with zero setup.
"""

import json
from pathlib import Path

FLAGGED_DATE = "2026-08-22"
FLAGGED_DEALER_GROUP = "midwest_dealers"

_STATE_PATH = Path(__file__).parent / "_floorplan_source_state.json"


def _read_state() -> dict:
    if not _STATE_PATH.exists():
        return {"corrected": []}
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"corrected": []}


def _write_state(state: dict) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def is_corrected(event_date: str, dealer_group: str) -> bool:
    if (event_date, dealer_group) != (FLAGGED_DATE, FLAGGED_DEALER_GROUP):
        return True
    return [event_date, dealer_group] in _read_state().get("corrected", [])


def mark_corrected(event_date: str = FLAGGED_DATE, dealer_group: str = FLAGGED_DEALER_GROUP) -> None:
    """Live-demo helper: simulates the dealer resending a corrected file."""
    state = _read_state()
    corrected = state.get("corrected", [])
    if [event_date, dealer_group] not in corrected:
        corrected.append([event_date, dealer_group])
    state["corrected"] = corrected
    _write_state(state)


def reset_source_state() -> None:
    """The only way the mock feed resets -- an operation on the source, never on Dagster."""
    _STATE_PATH.unlink(missing_ok=True)
