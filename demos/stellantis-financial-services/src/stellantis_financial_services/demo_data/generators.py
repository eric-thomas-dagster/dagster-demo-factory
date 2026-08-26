"""Deterministic synthetic data generators standing in for SFS's real feeds.

Every function below stands in for one of the ~700 SSIS packages being
migrated into Fabric pipelines. Dagster does not recompute their logic --
these generators are what demo mode returns *instead of* actually triggering
the Fabric pipeline and reading its output, per `templates/demo_mode_pattern.py`.

Generation is seeded from `(base_seed, event_date, ...)` so the same inputs
always produce the same rows -- repeat demo runs must not drift. Cardinalities
are illustrative for a mid-size captive auto lender (hundreds of dealers,
thousands of contracts/day), per the brief's realism note: volumes were not
stated by the AE, so exact figures here are not sourced and should not be
quoted as SFS's actual numbers.
"""

import hashlib

import numpy as np
import pandas as pd

DEMO_SEED = 20260826

DEALER_GROUPS = ["northeast_dealers", "midwest_dealers", "southeast_dealers", "west_dealers"]
_N_DEALERS = 240
CHANNELS = ["dealer_indirect", "direct_digital", "captive_program"]
BUREAUS = ["equifax", "experian", "transunion"]
SERVICERS = ["in_house_servicing", "third_party_servicer_a"]
PAYMENT_METHODS = ["ach", "card", "check", "dealer_remit"]


def _stable_seed(*parts) -> int:
    """Combine a base seed with partition context into one deterministic int.

    Python's built-in `hash()` is randomized per-process (PYTHONHASHSEED), so
    it cannot be used here -- a run-to-run stable digest is required.
    """
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def dealer_roster() -> pd.DataFrame:
    """The fixed roster of SFS-financed dealers -- stable across every run.

    Each dealer is deterministically assigned to one of the four regional
    floorplan-lending groups, standing in for SFS's actual dealer footprint
    (not sourced -- the brief gives no dealer count).
    """
    rng = np.random.default_rng(_stable_seed(DEMO_SEED, "dealer_roster"))
    rows = []
    for i in range(1, _N_DEALERS + 1):
        dealer_id = f"DLR-{i:04d}"
        group = DEALER_GROUPS[_stable_seed(dealer_id) % len(DEALER_GROUPS)]
        rows.append(
            {
                "dealer_id": dealer_id,
                "dealer_group": group,
                "dealer_name": f"{group.split('_')[0].title()} Auto Group #{i}",
                "credit_line_amount": round(float(rng.uniform(500_000, 6_000_000)), 2),
            }
        )
    return pd.DataFrame(rows)


def _vin(seed_key) -> str:
    digest = hashlib.sha256(str(seed_key).encode("utf-8")).hexdigest().upper()
    return "1FA" + digest[:14]


def generate_loan_originations(event_date: str, seed: int = DEMO_SEED) -> pd.DataFrame:
    """Stands in for the day's `raw_loan_originations` Fabric pipeline output."""
    rng = np.random.default_rng(_stable_seed(seed, "loan_orig", event_date))
    dealers = dealer_roster()
    n = int(rng.integers(550, 750))
    dealer_ids = rng.choice(dealers["dealer_id"], size=n)
    rows = []
    for i in range(n):
        loan_id = f"LN-{event_date}-{i:05d}"
        rows.append(
            {
                "loan_id": loan_id,
                "dealer_id": dealer_ids[i],
                "borrower_id": f"BOR-{event_date}-{i:05d}",
                "vehicle_vin": _vin((loan_id, "vin")),
                "origination_date": event_date,
                "principal_amount": round(max(float(rng.normal(28_000, 7_000)), 6_000.0), 2),
                "apr": round(float(rng.uniform(4.5, 11.5)), 3),
                "term_months": int(rng.choice([36, 48, 60, 72])),
                "channel": str(rng.choice(CHANNELS, p=[0.55, 0.30, 0.15])),
            }
        )
    return pd.DataFrame(rows)


def generate_lease_originations(event_date: str, seed: int = DEMO_SEED) -> pd.DataFrame:
    """Stands in for the day's `raw_lease_originations` Fabric pipeline output."""
    rng = np.random.default_rng(_stable_seed(seed, "lease_orig", event_date))
    dealers = dealer_roster()
    n = int(rng.integers(150, 260))
    dealer_ids = rng.choice(dealers["dealer_id"], size=n)
    rows = []
    for i in range(n):
        lease_id = f"LS-{event_date}-{i:05d}"
        msrp = round(max(float(rng.normal(38_000, 9_000)), 15_000.0), 2)
        rows.append(
            {
                "lease_id": lease_id,
                "dealer_id": dealer_ids[i],
                "borrower_id": f"BORL-{event_date}-{i:05d}",
                "vehicle_vin": _vin((lease_id, "vin")),
                "origination_date": event_date,
                "residual_value": round(msrp * float(rng.uniform(0.45, 0.62)), 2),
                "monthly_payment": round(float(rng.uniform(320, 780)), 2),
                "term_months": int(rng.choice([24, 36, 39])),
                "channel": str(rng.choice(CHANNELS, p=[0.55, 0.30, 0.15])),
            }
        )
    return pd.DataFrame(rows)


def generate_payment_transactions(event_date: str, seed: int = DEMO_SEED) -> pd.DataFrame:
    """Stands in for the day's `raw_payment_transactions` Fabric pipeline output.

    Payments are drawn against SFS's broader existing servicing book (not
    just same-day originations, which would still be in a funding window) --
    `contract_id` is a synthetic reference into that existing book, not a
    join key back to same-day origination rows.
    """
    rng = np.random.default_rng(_stable_seed(seed, "payments", event_date))
    n = int(rng.integers(2_600, 3_400))
    contract_ids = rng.integers(1, 55_000, size=n)
    contract_types = rng.choice(["loan", "lease"], size=n, p=[0.78, 0.22])
    rows = []
    for i in range(n):
        rows.append(
            {
                "transaction_id": f"TXN-{event_date}-{i:06d}",
                "contract_id": f"CONTRACT-{contract_ids[i]:06d}",
                "contract_type": str(contract_types[i]),
                "payment_date": event_date,
                "amount": round(float(rng.uniform(180, 950)), 2),
                "payment_method": str(rng.choice(PAYMENT_METHODS, p=[0.55, 0.20, 0.10, 0.15])),
                "servicer": str(rng.choice(SERVICERS, p=[0.7, 0.3])),
            }
        )
    return pd.DataFrame(rows)


def generate_credit_bureau_pull(event_date: str, seed: int = DEMO_SEED) -> pd.DataFrame:
    """Stands in for the day's `raw_credit_bureau_pull` Fabric pipeline output.

    One pull per new borrower originated that day (loan or lease).
    """
    rng = np.random.default_rng(_stable_seed(seed, "bureau", event_date))
    n = int(rng.integers(700, 950))
    rows = []
    for i in range(n):
        borrower_id = f"BOR-{event_date}-{i:05d}" if i % 3 else f"BORL-{event_date}-{i:05d}"
        rows.append(
            {
                "pull_id": f"CB-{event_date}-{i:05d}",
                "borrower_id": borrower_id,
                "bureau": str(rng.choice(BUREAUS)),
                "credit_score": int(np.clip(rng.normal(680, 55), 300, 850)),
                "pull_date": event_date,
            }
        )
    return pd.DataFrame(rows)


def generate_dealer_floorplan_feed(
    event_date: str,
    dealer_group: str,
    seed: int = DEMO_SEED,
    corrected: bool = True,
) -> pd.DataFrame:
    """Stands in for the day's `raw_dealer_floorplan_feed` Fabric pipeline output for one dealer group.

    When `corrected` is False, one row in the batch is missing `loan_id` --
    the planted anomaly `raw_dealer_floorplan_feed_completeness` catches. This
    never fires outside the one flagged (date, dealer_group) partition; see
    `demo_data/fabric_source_state.py`.
    """
    rng = np.random.default_rng(_stable_seed(seed, "floorplan", event_date, dealer_group))
    dealers = dealer_roster()
    group_dealers = dealers.loc[dealers["dealer_group"] == dealer_group, "dealer_id"].tolist()
    n = int(rng.integers(35, 70))
    dealer_ids = rng.choice(group_dealers, size=n) if group_dealers else []
    rows = []
    for i in range(n):
        advance_date = pd.Timestamp(event_date)
        loan_id = f"FP-{event_date}-{dealer_group}-{i:04d}"
        rows.append(
            {
                "floorplan_advance_id": f"FPA-{event_date}-{dealer_group}-{i:04d}",
                "dealer_id": dealer_ids[i] if len(dealer_ids) else None,
                "dealer_group": dealer_group,
                "vehicle_vin": _vin((loan_id, "vin")),
                "advance_date": event_date,
                "advance_amount": round(float(rng.uniform(18_000, 55_000)), 2),
                "curtailment_due_date": (advance_date + pd.Timedelta(days=90)).strftime("%Y-%m-%d"),
                "loan_id": loan_id,
            }
        )
    if not corrected and rows:
        rows[0]["loan_id"] = None
    return pd.DataFrame(rows)


def generate_delinquency_events(event_date: str, seed: int = DEMO_SEED) -> pd.DataFrame:
    """Stands in for the day's `stg_delinquency_events` Fabric pipeline output.

    Represents what the migrated delinquency-detection package already
    computes inside Fabric -- Dagster observes the result, it does not
    recompute the underlying payment-vs-due-date logic.
    """
    rng = np.random.default_rng(_stable_seed(seed, "delinquency", event_date))
    dealers = dealer_roster()
    n = int(rng.integers(40, 90))
    dealer_ids = rng.choice(dealers["dealer_id"], size=n)
    buckets = [30, 60, 90, 120]
    rows = []
    for i in range(n):
        rows.append(
            {
                "event_id": f"DLQ-{event_date}-{i:05d}",
                "contract_id": f"CONTRACT-{int(rng.integers(1, 55_000)):06d}",
                "dealer_id": dealer_ids[i],
                "days_past_due": int(rng.choice(buckets, p=[0.55, 0.25, 0.13, 0.07])),
                "delinquency_amount": round(float(rng.uniform(250, 2_400)), 2),
                "event_date": event_date,
            }
        )
    return pd.DataFrame(rows)
