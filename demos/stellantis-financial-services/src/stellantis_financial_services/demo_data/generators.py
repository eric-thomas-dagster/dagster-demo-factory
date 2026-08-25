"""Deterministic synthetic data generators.

Every function stands in for a real SFS vendor system named in the brief:

- `generate_loan_originations_frame`     -> dealer-submitted auto loan contracts
- `generate_lease_originations_frame`    -> dealer-submitted lease contracts
- `generate_payment_transactions_frame`  -> servicer payment/collections feed
- `generate_dealer_floorplan_frame`      -> dealer floorplan advance feed (the
                                             one genuinely flaky source -- see
                                             its retry policy in defs.yaml)
- `generate_credit_bureau_frame`         -> credit bureau pull for new borrowers

Generation is seeded from `(base_seed, event_date, ...)` so the same inputs
always produce the same rows -- repeat demo runs must not drift. Cardinalities
are "industry-typical auto-finance", per the brief's explicit flag that real
SFS volumes were not provided (treat as illustrative, not sourced).
"""

from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

DEALER_COUNT = 180
DEALER_IDS = [f"D-{i:04d}" for i in range(1, DEALER_COUNT + 1)]

CREDIT_BUREAUS = ["Experian", "TransUnion", "Equifax"]
STATES = ["MI", "OH", "IN", "IL", "WI", "PA", "TX", "FL", "CA", "GA"]


def _stable_seed(*parts: str | int) -> int:
    """Combine a base seed with partition context into one deterministic int.

    Python's built-in `hash()` is randomized per-process (PYTHONHASHSEED), so
    it cannot be used here -- a run-to-run stable digest is required.
    """
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def _dealer_for(rng: np.random.Generator) -> str:
    return str(rng.choice(DEALER_IDS))


def generate_loan_originations_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's dealer-submitted auto loan contracts."""
    rng = np.random.default_rng(_stable_seed(seed, "loan_originations", event_date))
    n = int(rng.integers(60, 140))
    rows = []
    for i in range(n):
        amount_financed = round(float(rng.uniform(9_500, 62_000)), 2)
        rows.append(
            {
                "loan_id": f"LN-{event_date.replace('-', '')}-{i:04d}",
                "contract_date": event_date,
                "dealer_id": _dealer_for(rng),
                "borrower_id": f"BOR-{event_date.replace('-', '')}-L{i:04d}",
                "vehicle_vin": f"1SF{rng.integers(10**13, 10**14 - 1)}",
                "amount_financed": amount_financed,
                "apr": round(float(rng.uniform(4.9, 21.9)), 3),
                "term_months": int(rng.choice([36, 48, 60, 72])),
                "product_type": "auto_loan",
                "borrower_state": str(rng.choice(STATES)),
            }
        )
    columns = [
        "loan_id", "contract_date", "dealer_id", "borrower_id", "vehicle_vin",
        "amount_financed", "apr", "term_months", "product_type", "borrower_state",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_lease_originations_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's dealer-submitted lease contracts."""
    rng = np.random.default_rng(_stable_seed(seed, "lease_originations", event_date))
    n = int(rng.integers(20, 55))
    rows = []
    for i in range(n):
        cap_cost = round(float(rng.uniform(24_000, 68_000)), 2)
        rows.append(
            {
                "lease_id": f"LE-{event_date.replace('-', '')}-{i:04d}",
                "contract_date": event_date,
                "dealer_id": _dealer_for(rng),
                "borrower_id": f"BOR-{event_date.replace('-', '')}-E{i:04d}",
                "vehicle_vin": f"1SF{rng.integers(10**13, 10**14 - 1)}",
                "capitalized_cost": cap_cost,
                "residual_value": round(cap_cost * float(rng.uniform(0.45, 0.62)), 2),
                "monthly_payment": round(cap_cost / int(rng.choice([24, 36, 39])) * 0.9, 2),
                "term_months": int(rng.choice([24, 36, 39])),
                "product_type": "lease",
                "borrower_state": str(rng.choice(STATES)),
            }
        )
    columns = [
        "lease_id", "contract_date", "dealer_id", "borrower_id", "vehicle_vin",
        "capitalized_cost", "residual_value", "monthly_payment", "term_months",
        "product_type", "borrower_state",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_payment_transactions_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's servicer payment/collections feed.

    Payments reference contracts from a trailing lookback window rather than
    tracking real cross-partition state -- `_stable_seed` makes the historical
    contract_ids for any past date reproducible without re-materializing it.
    """
    rng = np.random.default_rng(_stable_seed(seed, "payment_transactions", event_date))
    lookback_dates = list(pd.date_range(end=event_date, periods=45, freq="D").strftime("%Y-%m-%d"))

    candidate_contracts: list[tuple[str, str]] = []
    for past_date in lookback_dates:
        loans = generate_loan_originations_frame(past_date, seed)
        leases = generate_lease_originations_frame(past_date, seed)
        candidate_contracts.extend((cid, "auto_loan") for cid in loans["loan_id"])
        candidate_contracts.extend((cid, "lease") for cid in leases["lease_id"])

    n = int(rng.integers(900, 1_500))
    choices = rng.integers(0, len(candidate_contracts), size=n)
    rows = []
    for i, idx in enumerate(choices):
        contract_id, contract_type = candidate_contracts[int(idx)]
        days_past_due = int(rng.choice([0, 0, 0, 0, 5, 15, 30, 45, 60], p=[0.55, 0.1, 0.1, 0.05, 0.06, 0.05, 0.04, 0.03, 0.02]))
        amount_paid = round(float(rng.uniform(180, 950)), 2) if days_past_due < 60 else 0.0
        rows.append(
            {
                "payment_id": f"PMT-{event_date.replace('-', '')}-{i:05d}",
                "contract_id": contract_id,
                "contract_type": contract_type,
                "payment_date": event_date,
                "amount_paid": amount_paid,
                "days_past_due": days_past_due,
                "payment_method": str(rng.choice(["ach", "card", "check", "portal"])),
            }
        )
    columns = ["payment_id", "contract_id", "contract_type", "payment_date", "amount_paid", "days_past_due", "payment_method"]
    return pd.DataFrame(rows, columns=columns)


def generate_dealer_floorplan_frame(event_date: str, seed: int, corrected: bool) -> pd.DataFrame:
    """One day's dealer floorplan (inventory financing) advance feed.

    The genuinely flaky source in this demo -- floorplan advance batches are
    dealer-submitted, not system-to-system, so `raw_dealer_floorplan_feed`
    carries a real `RetryPolicy` (see its `defs.yaml`).

    `corrected=False` for the one planted-anomaly (feed, date) reproduces the
    exact malformed batch a real dealer sent: one advance record missing its
    VIN. `corrected=True` (the vendor having resent the file) is the same
    batch with that one field filled in -- same row count, same advance IDs,
    nothing else about the asset changes.
    """
    rng = np.random.default_rng(_stable_seed(seed, "dealer_floorplan", event_date))
    n = int(rng.integers(25, 65))
    rows = []
    for i in range(n):
        vin = f"1SF{rng.integers(10**13, 10**14 - 1)}"
        if not corrected and i == 0:
            vin = None
        rows.append(
            {
                "floorplan_advance_id": f"FP-{event_date.replace('-', '')}-{i:04d}",
                "dealer_id": _dealer_for(rng),
                "advance_date": event_date,
                "vehicle_vin": vin,
                "advance_amount": round(float(rng.uniform(15_000, 48_000)), 2),
                "curtailment_due_date": str(
                    (pd.Timestamp(event_date) + pd.Timedelta(days=90)).date()
                ),
            }
        )
    columns = ["floorplan_advance_id", "dealer_id", "advance_date", "vehicle_vin", "advance_amount", "curtailment_due_date"]
    return pd.DataFrame(rows, columns=columns)


def generate_credit_bureau_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's credit bureau pull, one row per new loan/lease borrower."""
    rng = np.random.default_rng(_stable_seed(seed, "credit_bureau", event_date))
    loans = generate_loan_originations_frame(event_date, seed)
    leases = generate_lease_originations_frame(event_date, seed)
    borrower_ids = list(loans["borrower_id"]) + list(leases["borrower_id"])

    rows = []
    for borrower_id in borrower_ids:
        rows.append(
            {
                "borrower_id": borrower_id,
                "bureau_name": str(rng.choice(CREDIT_BUREAUS)),
                "bureau_score": int(rng.integers(560, 830)),
                "score_date": event_date,
                "inquiry_count_6mo": int(rng.integers(0, 6)),
            }
        )
    columns = ["borrower_id", "bureau_name", "bureau_score", "score_date", "inquiry_count_6mo"]
    return pd.DataFrame(rows, columns=columns)
