"""Demo-mode constants and the planted anomaly's self-healing state.

The anomaly is modelled as *source arrival timing*, per
`templates/demo_mode_pattern.py`: the carrier's rate API simply has no rows
for `ANOMALY_CARRIER` on `ANOMALY_DATE` the first time it's read. That first
read also marks the partition "received" in a local state file, so a second
read of the same partition -- the rematerialize in the recovery money shot --
finds the data has since landed. No heal step, no control asset: the
partition is idempotent, and the source's own state is what changed.

State lives outside the Dagster-tracked object graph entirely (a JSON file
under `demo_data/`), never as metadata on a Dagster asset -- seeing a
disconnected `healed_partitions` control node in the asset graph is the
clearest possible tell that a prospect is looking at scaffolding.
"""

from __future__ import annotations

import json
from pathlib import Path

EXPECTED_CARRIERS = ["fedex", "ups", "regional_ltl_a", "regional_ltl_b"]

ANOMALY_CARRIER = "regional_ltl_b"
ANOMALY_DATE = "2026-08-21"

# DailyPartitionsDefinition's end_date is EXCLUSIVE of the final partition, so
# a partitions_def ending 2026-08-25 has 2026-08-23 as its last key. This
# window must match that or per-partition materialization fails on a key the
# partitions def doesn't know about.
DEMO_WINDOW_START = "2026-08-15"
DEMO_WINDOW_END = "2026-08-23"

_SOURCE_STATE_PATH = Path(__file__).parent / "_source_state.json"


def _read_state() -> dict:
    if not _SOURCE_STATE_PATH.exists():
        return {}
    try:
        return json.loads(_SOURCE_STATE_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def source_has_arrived(carrier: str, event_date: str) -> bool:
    """Whether the simulated carrier API currently has rows for this partition.

    Every partition except the planted anomaly always has data. The anomaly
    partition has none until it has been read once -- that first read is
    also what marks it arrived, standing in for "the carrier's feed lands a
    couple of hours after the first check."
    """
    if not (carrier == ANOMALY_CARRIER and event_date == ANOMALY_DATE):
        return True
    return event_date in _read_state().get("arrived_late", [])


def mark_source_arrived(carrier: str, event_date: str) -> None:
    if not (carrier == ANOMALY_CARRIER and event_date == ANOMALY_DATE):
        return
    state = _read_state()
    arrived = set(state.get("arrived_late", []))
    arrived.add(event_date)
    state["arrived_late"] = sorted(arrived)
    _SOURCE_STATE_PATH.write_text(json.dumps(state, indent=2))


def reset_source_state() -> None:
    """The only way the demo resets -- an operation on the mock source, never on Dagster."""
    _SOURCE_STATE_PATH.unlink(missing_ok=True)
