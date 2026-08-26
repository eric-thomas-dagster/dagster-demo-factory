"""Deterministic synthetic data generators.

Every function stands in for one of SFS's vendor-file bronze feeds named in
the brief, landing into what will become Fabric pipeline / lakehouse tables:

- `generate_loan_originations_frame`     -> daily loan origination feed
- `generate_lease_originations_frame`    -> daily lease origination feed
- `generate_payment_transactions_frame`  -> daily payment/collections feed
                                             against the existing back book
- `generate_dealer_floorplan_feed_frame` -> per-region dealer floorplan feed
                                             (the one asset with a genuine
                                             `dealer_group` partition)
- `generate_credit_bureau_pull_frame`    -> daily bureau pull for that day's
                                             loan/lease applicants

Generation is seeded from `(base_seed, feed_name, event_date, ...)` so the
same inputs always produce the same rows -- repeat demo runs must not drift.
Cardinalities are "mid-size captive auto lender", per the brief's explicit
flag that SFS's real volumes were not stated (illustrative, not sourced);
per-partition row counts are scaled down from the brief's thousands-per-day
order of magnitude so the demo materializes quickly on a shared screen.
"""

import hashlib

import numpy as np
import pandas as pd

DEALER_GROUPS = ["midwest", "northeast", "south", "west"]
DEALERS_PER_GROUP = 15

STATES_BY_GROUP = {
    "midwest": ["OH", "IN", "IL", "MI", "WI"],
    "northeast": ["NY", "NJ", "PA", "CT", "MA"],
    "south": ["TX", "FL", "GA", "NC", "TN"],
    "west": ["CA", "AZ", "WA", "CO", "NV"],
}

VEHICLE_MODELS = [
    "Jeep Grand Cherokee", "Ram 1500", "Chrysler Pacifica", "Dodge Durango",
    "Jeep Wrangler", "Fiat 500e", "Alfa Romeo Giulia", "Jeep Compass",
]

BUREAUS = ["Experian Auto", "Equifax Auto", "TransUnion Auto"]

# Fixed back-book of active contracts that make payments daily -- independent
# of any single day's originations, standing in for SFS's existing ~700-
# package portfolio of loans/leases already on the books.
ACCOUNT_POOL_SIZE = 5_000
ACCOUNT_IDS = [f"ACCT-{i:07d}" for i in range(1, ACCOUNT_POOL_SIZE + 1)]


def _stable_seed(*parts: str | int) -> int:
    """Combine a base seed with partition context into one deterministic int.

    Python's built-in `hash()` is randomized per-process (PYTHONHASHSEED), so
    it cannot be used here -- a run-to-run stable digest is required.
    """
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def _dealer_ids(dealer_group: str) -> list[str]:
    idx = DEALER_GROUPS.index(dealer_group)
    start = idx * DEALERS_PER_GROUP + 1
    return [f"DLR-{i:05d}" for i in range(start, start + DEALERS_PER_GROUP)]


def _account_dealer_group(account_id: str) -> str:
    """Deterministically assign each back-book account to one dealer_group."""
    digest = hashlib.sha256(account_id.encode("utf-8")).digest()
    return DEALER_GROUPS[digest[0] % len(DEALER_GROUPS)]


def generate_loan_originations_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's retail auto loan originations across all four regions."""
    rng = np.random.default_rng(_stable_seed(seed, "loan_originations", event_date))
    n = int(rng.integers(180, 321))
    rows = []
    for i in range(n):
        dealer_group = str(rng.choice(DEALER_GROUPS))
        dealer_id = str(rng.choice(_dealer_ids(dealer_group)))
        principal = round(float(rng.uniform(14_000, 68_000)), 2)
        term = int(rng.choice([36, 48, 60, 72]))
        rows.append(
            {
                "loan_id": f"LOAN-{event_date.replace('-', '')}-{i:05d}",
                "origination_date": event_date,
                "dealer_id": dealer_id,
                "dealer_group": dealer_group,
                "borrower_id": f"BOR-{event_date.replace('-', '')}-L{i:05d}",
                "vehicle_model": str(rng.choice(VEHICLE_MODELS)),
                "principal_amount": principal,
                "apr": round(float(rng.uniform(5.5, 14.5)), 3),
                "term_months": term,
                "state": str(rng.choice(STATES_BY_GROUP[dealer_group])),
            }
        )
    columns = [
        "loan_id", "origination_date", "dealer_id", "dealer_group", "borrower_id",
        "vehicle_model", "principal_amount", "apr", "term_months", "state",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_lease_originations_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's lease originations across all four regions."""
    rng = np.random.default_rng(_stable_seed(seed, "lease_originations", event_date))
    n = int(rng.integers(45, 96))
    rows = []
    for i in range(n):
        dealer_group = str(rng.choice(DEALER_GROUPS))
        dealer_id = str(rng.choice(_dealer_ids(dealer_group)))
        cap_cost = round(float(rng.uniform(22_000, 72_000)), 2)
        rows.append(
            {
                "lease_id": f"LEASE-{event_date.replace('-', '')}-{i:05d}",
                "origination_date": event_date,
                "dealer_id": dealer_id,
                "dealer_group": dealer_group,
                "borrower_id": f"BOR-{event_date.replace('-', '')}-S{i:05d}",
                "vehicle_model": str(rng.choice(VEHICLE_MODELS)),
                "capitalized_cost": cap_cost,
                "residual_value": round(cap_cost * float(rng.uniform(0.45, 0.62)), 2),
                "money_factor": round(float(rng.uniform(0.0009, 0.0025)), 5),
                "term_months": int(rng.choice([24, 36, 39])),
                "state": str(rng.choice(STATES_BY_GROUP[dealer_group])),
            }
        )
    columns = [
        "lease_id", "origination_date", "dealer_id", "dealer_group", "borrower_id",
        "vehicle_model", "capitalized_cost", "residual_value", "money_factor",
        "term_months", "state",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_payment_transactions_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's payment/collections activity against the existing back book."""
    rng = np.random.default_rng(_stable_seed(seed, "payment_transactions", event_date))
    n = int(rng.integers(650, 1_051))
    account_ids = rng.choice(ACCOUNT_IDS, size=n, replace=False)
    rows = []
    for account_id in account_ids:
        amount_due = round(float(rng.uniform(280, 950)), 2)
        # ~8% of the back book is delinquent on any given day -- illustrative,
        # not sourced; matches the brief's flag that real delinquency rates
        # weren't provided.
        days_past_due = int(rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 30, 45, 60, 90], size=1)[0])
        paid_fraction = 1.0 if days_past_due == 0 else float(rng.uniform(0.0, 0.6))
        rows.append(
            {
                "payment_id": f"PMT-{event_date.replace('-', '')}-{account_id[-7:]}",
                "account_id": str(account_id),
                "dealer_group": _account_dealer_group(str(account_id)),
                "payment_date": event_date,
                "amount_due": amount_due,
                "amount_paid": round(amount_due * paid_fraction, 2),
                "days_past_due": days_past_due,
            }
        )
    columns = [
        "payment_id", "account_id", "dealer_group", "payment_date",
        "amount_due", "amount_paid", "days_past_due",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_dealer_floorplan_feed_frame(event_date: str, dealer_group: str, seed: int) -> pd.DataFrame:
    """One region's one day of dealer floorplan financing feed.

    Carries `arrival_hour` -- the hour (0-23, vendor local time) the feed
    actually landed -- so the lateness check has a real value to compare
    against an expected-by-hour SLA, computed from genuine synthesized data
    rather than a planted anomaly.
    """
    rng = np.random.default_rng(_stable_seed(seed, "dealer_floorplan_feed", event_date, dealer_group))
    rows = []
    for dealer_id in _dealer_ids(dealer_group):
        units = int(rng.integers(8, 65))
        advance_per_unit = round(float(rng.uniform(24_000, 46_000)), 2)
        rows.append(
            {
                "dealer_id": dealer_id,
                "dealer_group": dealer_group,
                "feed_date": event_date,
                "units_floored": units,
                "floorplan_balance": round(units * advance_per_unit, 2),
                "curtailment_due_amount": round(float(rng.uniform(0, 40_000)), 2),
                # Most regions land well inside the overnight batch window;
                # one region runs consistently later -- a real, computable
                # timing signal for the lateness check, not a planted failure.
                "arrival_hour": int(rng.integers(3, 6)) if dealer_group != "south" else int(rng.integers(7, 10)),
            }
        )
    columns = [
        "dealer_id", "dealer_group", "feed_date", "units_floored",
        "floorplan_balance", "curtailment_due_amount", "arrival_hour",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_credit_bureau_pull_frame(event_date: str, seed: int) -> pd.DataFrame:
    """One day's bureau pulls for that day's loan + lease applicants."""
    loans = generate_loan_originations_frame(event_date, seed)
    leases = generate_lease_originations_frame(event_date, seed)
    applicants = pd.concat([loans[["borrower_id"]], leases[["borrower_id"]]], ignore_index=True)

    rng = np.random.default_rng(_stable_seed(seed, "credit_bureau_pull", event_date))
    rows = []
    for i, borrower_id in enumerate(applicants["borrower_id"]):
        rows.append(
            {
                "pull_id": f"PULL-{event_date.replace('-', '')}-{i:05d}",
                "borrower_id": borrower_id,
                "bureau_name": str(rng.choice(BUREAUS)),
                "pull_date": event_date,
                "credit_score": int(rng.integers(560, 821)),
            }
        )
    columns = ["pull_id", "borrower_id", "bureau_name", "pull_date", "credit_score"]
    return pd.DataFrame(rows, columns=columns)
