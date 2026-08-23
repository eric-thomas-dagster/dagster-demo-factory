"""Tracks which anomalous partitions have been "healed" during a demo run.

The planted anomaly (missing `regional_ltl_b` carrier data for `2026-08-21`) is
seeded deterministically, so a naive rematerialize would regenerate the same
missing data every time. The money-shot recovery sequence needs a partition to
go from broken to clean without editing YAML or touching a terminal -- so the
healed set lives in a small JSON file that a Dagster op can write to, and every
generator consults before deciding whether to inject the anomaly.

Real dbt / Snowflake code paths never call anything in this module -- healing
is a demo_mode-only concept.
"""

import json
from pathlib import Path

STATE_DIR = Path(__file__).parent / ".demo_state"
HEALED_PARTITIONS_PATH = STATE_DIR / "healed_partitions.json"

ANOMALY_DATE = "2026-08-21"
ANOMALY_CARRIER = "regional_ltl_b"


def _read_healed_dates() -> list[str]:
    if not HEALED_PARTITIONS_PATH.exists():
        return []
    return json.loads(HEALED_PARTITIONS_PATH.read_text(encoding="utf-8"))


def is_healed(rate_date: str) -> bool:
    """Whether the anomaly for `rate_date` has been healed in this demo run."""
    return rate_date in _read_healed_dates()


def mark_healed(rate_date: str) -> None:
    """Record that `rate_date` should generate clean data from now on."""
    healed_dates = set(_read_healed_dates())
    healed_dates.add(rate_date)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    HEALED_PARTITIONS_PATH.write_text(
        json.dumps(sorted(healed_dates)), encoding="utf-8"
    )


def reset_healed_state() -> None:
    """Clear all healed partitions so the demo can be run again from scratch."""
    if HEALED_PARTITIONS_PATH.exists():
        HEALED_PARTITIONS_PATH.unlink()
