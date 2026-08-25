"""Deterministic synthetic data generators.

Every function stands in for a real ISO-NE system named in the brief:

- `generate_readings_frame`  -> the legacy Oracle interval-telemetry extract
- `generate_advisory_frame`  -> the external operations-advisory feed the
                                 sensor watches

Generation is seeded from `(base_seed, event_date, ...)` so the same inputs
always produce the same rows -- repeat demo runs must not drift. Cardinalities
and value ranges are modest and clearly illustrative, per the brief: no real
ISO-NE volumes or figures were provided, so none are invented here.
"""

from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

# Generic, utility-flavored reporting points -- not a named real ISO-NE
# facility or interconnection. The brief has no confirmed business domain
# behind this pipeline, so these stand in for "wherever telemetry comes from"
# without guessing which.
REPORTING_POINTS = [f"RP-{i:03d}" for i in range(1, 13)]
REGIONS = ["CT", "ME", "MA", "NH", "RI", "VT"]

ADVISORY_CATEGORIES = [
    "capacity_alert",
    "transmission_notice",
    "market_notice",
    "weather_advisory",
]

# A couple of days in the demo window carry more advisory volume than usual --
# an illustrative gesture at real operational variability, not a modeled
# incident.
ELEVATED_ADVISORY_DATES = {"2026-08-17", "2026-08-22"}


def _stable_seed(*parts: str | int) -> int:
    """Combine a base seed with partition context into one deterministic int.

    Python's built-in `hash()` is randomized per-process (PYTHONHASHSEED), so
    it cannot be used here -- a run-to-run stable digest is required.
    """
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def generate_readings_frame(event_date: str, seed: int) -> pd.DataFrame:
    """Stands in for one day's interval-telemetry extract from the legacy Oracle system.

    One row per reporting point per hour -- illustrative volume, not a real
    ISO-NE reporting cadence (unspecified in the brief).
    """
    rng = np.random.default_rng(_stable_seed(seed, "readings", event_date))
    rows = []
    for point in REPORTING_POINTS:
        base_mw = float(rng.uniform(40, 220))
        for hour in range(24):
            diurnal = 1.0 + 0.35 * np.sin((hour - 6) / 24 * 2 * np.pi)
            reading = round(base_mw * diurnal + float(rng.normal(0, 3)), 2)
            rows.append(
                {
                    "reporting_point_id": point,
                    "event_date": event_date,
                    "interval_ending": f"{event_date}T{hour:02d}:00:00Z",
                    "reading_mw": max(reading, 0.0),
                    "quality_flag": "estimated" if rng.random() < 0.03 else "good",
                    "source_system": "ORACLE_LEGACY",
                }
            )
    return pd.DataFrame(rows)


def generate_advisory_frame(event_date: str, seed: int) -> pd.DataFrame:
    """Stands in for one day's operations-advisory feed -- the external system
    `external_feed_raw`'s sensor watches for new arrivals.
    """
    rng = np.random.default_rng(_stable_seed(seed, "advisory", event_date))
    # Roughly a third of days are quiet (zero advisories) so `platform_status`
    # actually varies between "nominal" and "advisory_active" -- an
    # always-active status would say nothing.
    base_count = int(rng.integers(0, 5))
    count = base_count * 3 if event_date in ELEVATED_ADVISORY_DATES else base_count

    rows = []
    for i in range(count):
        category = rng.choice(ADVISORY_CATEGORIES)
        region = rng.choice(REGIONS)
        severity = rng.choice(["info", "watch", "warning"], p=[0.6, 0.3, 0.1])
        rows.append(
            {
                "notice_id": f"ADV-{event_date.replace('-', '')}-{i:03d}",
                "event_date": event_date,
                "issued_at": f"{event_date}T{int(rng.integers(0, 24)):02d}:{int(rng.integers(0, 59)):02d}:00Z",
                "category": str(category),
                "severity": str(severity),
                "region": str(region),
                "message_summary": f"{str(category).replace('_', ' ').title()} for {region} region",
            }
        )
    columns = ["notice_id", "event_date", "issued_at", "category", "severity", "region", "message_summary"]
    return pd.DataFrame(rows, columns=columns)
