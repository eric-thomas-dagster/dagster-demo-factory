"""Synthetic stand-ins for the two feeds behind the target Iceberg/Polaris lake.

No price/corporate-action vendor is named in the brief ("vendor unnamed, no
credentials exist" -- see brief's Demo mode section), so these generators
simulate the vendor feed itself, not a real one. Sized to the POC's own
stated scope -- "migrate one index end-to-end" -- rather than MarketGrader's
full ~40,000-company universe: 60 constituent-candidate symbols is legible on
a shared screen and still large enough that the arrival/completeness checks
mean something.

Deterministic: every generator is seeded from `(kind, partition_date)` only,
so the same partition always produces the same row count and the same
symbol set -- required by house rules ("Row counts must not drift between
runs") and by `validate_e2e.py`, which asserts exact counts.
"""

import datetime
import hashlib
import os
from pathlib import Path

import pyarrow as pa

# Local, zero-setup stand-in for the target Iceberg/Polaris lake -- a
# PyIceberg SQL (SQLite) catalog with a file:// warehouse, both created on
# first use inside the project. Real credentials for an actual Polaris/REST
# catalog are never required to run this demo. Path defaults inside the
# project (`demo_data/iceberg_warehouse/`), matching house rules ("storage
# paths default to somewhere inside the project"); the env var overrides,
# never gates, startup.
_DEFAULT_WAREHOUSE_DIR = Path(__file__).parent / "iceberg_warehouse"


def demo_iceberg_warehouse_dir() -> Path:
    path = Path(os.environ.get("MARKETGRADER_DEMO_ICEBERG_WAREHOUSE_DIR", str(_DEFAULT_WAREHOUSE_DIR)))
    path.mkdir(parents=True, exist_ok=True)
    return path


def demo_iceberg_catalog_properties() -> dict[str, str]:
    warehouse_dir = demo_iceberg_warehouse_dir()
    return {
        "uri": f"sqlite:///{warehouse_dir / 'catalog.db'}",
        "warehouse": f"file://{warehouse_dir}",
    }

# One-index POC universe -- a legible subset standing in for MarketGrader's
# ~40,000-company scoring universe (public research; not contradicted by the
# AE notes). Fixed list, not randomly sampled, so every partition scores the
# same symbols -- a real day-over-day constituent-bounds check needs a stable
# universe to compare against.
UNIVERSE = [f"MG{i:03d}" for i in range(1, 61)]

CORPORATE_ACTION_TYPES = ["dividend", "split", "spinoff", "name_change"]


def _seeded_rng_state(kind: str, partition_date: str) -> int:
    digest = hashlib.sha256(f"marketgrader|{kind}|{partition_date}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def generate_price_feed(partition_date: str) -> pa.Table:
    """One row per universe symbol for the given date -- deterministic price,
    always present (the checks exist to catch a *missing* file, not to
    demonstrate one; every partition here lands complete)."""
    seed = _seeded_rng_state("price", partition_date)
    d = datetime.date.fromisoformat(partition_date)

    symbols = list(UNIVERSE)
    prices = []
    for i, sym in enumerate(symbols):
        # Deterministic pseudo-price: stable base per symbol, small
        # date-dependent drift so the same symbol's price still moves
        # day-to-day without a random number generator.
        base = 20.0 + (hash(sym) % 4800) / 10.0
        drift = ((seed >> (i % 32)) % 401 - 200) / 1000.0  # +/- 0.2
        prices.append(round(base + drift, 2))

    return pa.table(
        {
            "symbol": pa.array(symbols, type=pa.string()),
            "price": pa.array(prices, type=pa.float64()),
            "currency": pa.array(["USD"] * len(symbols), type=pa.string()),
            "date": pa.array([d] * len(symbols), type=pa.date32()),
        }
    )


def generate_corporate_actions(partition_date: str) -> pa.Table:
    """Sparse: most days have zero-to-a-handful of corporate actions across a
    60-symbol universe -- an empty-but-present partition is still a complete
    (checks-passing) partition, matching how corporate-action feeds actually
    behave."""
    seed = _seeded_rng_state("corp_actions", partition_date)
    d = datetime.date.fromisoformat(partition_date)

    action_count = seed % 4  # 0-3 actions on a given day
    symbols, action_types, details = [], [], []
    for i in range(action_count):
        idx = (seed >> (i * 8)) % len(UNIVERSE)
        symbols.append(UNIVERSE[idx])
        action_type = CORPORATE_ACTION_TYPES[(seed >> (i * 8 + 3)) % len(CORPORATE_ACTION_TYPES)]
        action_types.append(action_type)
        details.append(f"{action_type} recorded for {UNIVERSE[idx]} on {partition_date}")

    return pa.table(
        {
            "symbol": pa.array(symbols, type=pa.string()),
            "action_type": pa.array(action_types, type=pa.string()),
            "details": pa.array(details, type=pa.string()),
            "date": pa.array([d] * action_count, type=pa.date32()),
        }
    )


def constituent_count_for_date(partition_date: str) -> int:
    """Deterministic stand-in for "how many symbols made it into today's
    index constituent list" -- used by both `barrons_400_constituents`
    (graph-first, no real body) and its day-over-day bounds check, so the
    check evaluates a real, reproducible number rather than a hardcoded
    pass. Stays within a tight band of the universe size, exactly like a
    real index's constituent count from day to day."""
    seed = _seeded_rng_state("constituents", partition_date)
    return len(UNIVERSE) - (seed % 4)  # 57-60 constituents
