"""Deterministic reference data shared across generators.

Real mode: the dealer list and portfolio scale come from SFS's own systems
(the dealer master in Fabric, the servicing book). Demo mode needs something
stable to derive both the dealer-floorplan feed and the `dim_dealer` rollup
from without either one drifting from the other between runs -- so both
read from the same `dealer_roster()` here rather than rolling their own.
"""

from __future__ import annotations

import hashlib
from datetime import date as _date

import numpy as np

from stellantis_financial_services.components.partitions import DEALER_GROUPS

_PORTFOLIO_EPOCH = _date(2026, 7, 1)
_DEALERS_PER_GROUP = {"midwest": 210, "northeast": 145, "south": 190, "west": 160}
_STATES_BY_GROUP = {
    "midwest": ["MI", "OH", "IN", "IL", "WI"],
    "northeast": ["NY", "PA", "NJ", "MA", "CT"],
    "south": ["TX", "GA", "FL", "NC", "TN"],
    "west": ["CA", "AZ", "WA", "CO", "NV"],
}


def rng(seed: int, *parts: str) -> np.random.Generator:
    """A `Generator` seeded deterministically from `seed` and any number of
    string parts (asset name, partition key, ...). Same inputs -> same
    numbers, every run, forever -- required so the demo never drifts between
    audiences.
    """
    digest = hashlib.sha256(f"{seed}:{':'.join(parts)}".encode()).digest()
    return np.random.default_rng(int.from_bytes(digest[:4], "big"))


def dealer_roster(dealer_group: str, seed: int = 20260826) -> list[dict]:
    """The (static) list of dealers reporting floorplan balances in one region."""
    count = _DEALERS_PER_GROUP[dealer_group]
    states = _STATES_BY_GROUP[dealer_group]
    generator = rng(seed, "dealer_roster", dealer_group)
    return [
        {
            "dealer_id": f"DLR-{dealer_group[:2].upper()}-{i:04d}",
            "dealer_name": f"{dealer_group.title()} Motors #{i:04d}",
            "dealer_group": dealer_group,
            "state": states[int(generator.integers(0, len(states)))],
            "floorplan_limit": float(generator.integers(500_000, 4_000_000)),
        }
        for i in range(1, count + 1)
    ]


def all_dealers() -> list[dict]:
    rows: list[dict] = []
    for group in DEALER_GROUPS:
        rows.extend(dealer_roster(group))
    return rows


def portfolio_day_index(date: str) -> int:
    return (_date.fromisoformat(date) - _PORTFOLIO_EPOCH).days


def portfolio_total_contracts(date: str) -> int:
    """Slow, steady book growth -- a captive lender's active servicing book
    doesn't reset daily the way an origination batch does."""
    return 42_000 + portfolio_day_index(date) * 6


def portfolio_avg_balance(date: str) -> float:
    generator = rng(20260826, "portfolio_avg_balance", date)
    return float(generator.uniform(23_500, 25_500))


def portfolio_total_balance(date: str) -> float:
    return portfolio_total_contracts(date) * portfolio_avg_balance(date)
