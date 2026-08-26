"""Deterministic synthetic data generators.

Every function stands in for a real Kapitus vendor system named in the brief:

- `generate_loan_applications_frame`   -> Fivetran-sourced loan origination
                                           system (the funding decision record)
- `generate_bank_statement_frame`      -> OCR/analytics-derived bank statement
                                           analysis, landed to S3
- `generate_credit_bureau_frame`       -> commercial credit bureau pull,
                                           landed to S3 via Lambda

Each is generated per `(event_date, product_line)` -- the two partition
dimensions every bronze asset shares. Generation is seeded from
`(base_seed, event_date, product_line, ...)` so the same inputs always
produce the same rows -- repeat demo runs must not drift. Cardinalities are
"industry-typical small-business lending", per the brief's explicit flag
that real Kapitus daily volumes were not provided (illustrative, not sourced).
"""

from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

BUSINESS_COUNT = 4_000
BUSINESS_IDS = [f"BIZ-{i:06d}" for i in range(1, BUSINESS_COUNT + 1)]

BUREAUS = ["Experian Business", "Equifax Business", "Dun & Bradstreet"]
STATES = ["NY", "NJ", "CT", "PA", "FL", "TX", "CA", "IL", "GA", "OH"]

# (min_amount, max_amount, apr_min, apr_max, term_months or None for revolving)
_PRODUCT_LINE_SHAPE = {
    "term_loan": (10_000, 250_000, 8.0, 18.0, (12, 24, 36, 48, 60)),
    "revenue_based_financing": (10_000, 150_000, 20.0, 50.0, (6, 9, 12, 18)),
    "equipment_financing": (15_000, 500_000, 7.0, 16.0, (24, 36, 48, 60, 84)),
    "sba_loan": (50_000, 5_000_000, 8.0, 13.0, (60, 84, 120)),
    "line_of_credit": (10_000, 250_000, 10.0, 24.0, None),
}


def _stable_seed(*parts: str | int) -> int:
    """Combine a base seed with partition context into one deterministic int.

    Python's built-in `hash()` is randomized per-process (PYTHONHASHSEED), so
    it cannot be used here -- a run-to-run stable digest is required.
    """
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def generate_loan_applications_frame(event_date: str, product_line: str, seed: int) -> pd.DataFrame:
    """One (date, product_line) batch of loan applications and their funding decisions."""
    rng = np.random.default_rng(_stable_seed(seed, "loan_applications", event_date, product_line))
    min_amt, max_amt, apr_min, apr_max, terms = _PRODUCT_LINE_SHAPE[product_line]
    n = int(rng.integers(15, 46))

    rows = []
    for i in range(n):
        business_id = str(rng.choice(BUSINESS_IDS))
        requested_amount = round(float(rng.uniform(min_amt, max_amt)), 2)
        approved = bool(rng.random() < 0.72)
        funded_amount = round(requested_amount * float(rng.uniform(0.7, 1.0)), 2) if approved else None
        rows.append(
            {
                "application_id": f"APP-{product_line[:4].upper()}-{event_date.replace('-', '')}-{i:04d}",
                "application_date": event_date,
                "product_line": product_line,
                "business_id": business_id,
                "business_state": str(rng.choice(STATES)),
                "requested_amount": requested_amount,
                "funding_status": "funded" if approved else "declined",
                "funded_amount": funded_amount,
                "apr": round(float(rng.uniform(apr_min, apr_max)), 3) if approved else None,
                "term_months": (int(rng.choice(terms)) if terms and approved else None),
            }
        )
    columns = [
        "application_id", "application_date", "product_line", "business_id", "business_state",
        "requested_amount", "funding_status", "funded_amount", "apr", "term_months",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_bank_statement_frame(event_date: str, product_line: str, seed: int) -> pd.DataFrame:
    """One (date, product_line) batch of OCR-derived bank statement analysis, one row per applicant."""
    rng = np.random.default_rng(_stable_seed(seed, "bank_statement_data", event_date, product_line))
    applications = generate_loan_applications_frame(event_date, product_line, seed)

    rows = []
    for i, business_id in enumerate(applications["business_id"]):
        avg_daily_balance = round(float(rng.uniform(2_000, 150_000)), 2)
        rows.append(
            {
                "statement_id": f"STMT-{product_line[:4].upper()}-{event_date.replace('-', '')}-{i:04d}",
                "statement_date": event_date,
                "product_line": product_line,
                "business_id": business_id,
                "avg_daily_balance": avg_daily_balance,
                "nsf_count_90d": int(rng.choice([0, 0, 0, 1, 1, 2, 3], p=[0.5, 0.15, 0.1, 0.1, 0.07, 0.05, 0.03])),
                "monthly_revenue_estimate": round(float(rng.uniform(10_000, 800_000)), 2),
                "cash_flow_score": int(rng.integers(1, 101)),
            }
        )
    columns = [
        "statement_id", "statement_date", "product_line", "business_id",
        "avg_daily_balance", "nsf_count_90d", "monthly_revenue_estimate", "cash_flow_score",
    ]
    return pd.DataFrame(rows, columns=columns)


def generate_credit_bureau_frame(event_date: str, product_line: str, seed: int) -> pd.DataFrame:
    """One (date, product_line) batch of commercial credit bureau pulls, one row per applicant."""
    rng = np.random.default_rng(_stable_seed(seed, "credit_bureau_pulls", event_date, product_line))
    applications = generate_loan_applications_frame(event_date, product_line, seed)

    rows = []
    for i, business_id in enumerate(applications["business_id"]):
        rows.append(
            {
                "pull_id": f"PULL-{product_line[:4].upper()}-{event_date.replace('-', '')}-{i:04d}",
                "pull_date": event_date,
                "product_line": product_line,
                "business_id": business_id,
                "bureau_name": str(rng.choice(BUREAUS)),
                "business_credit_score": int(rng.integers(500, 801)),
                "personal_credit_score": int(rng.integers(580, 821)),
                "years_in_business": round(float(rng.uniform(0.5, 25.0)), 1),
                "existing_debt_obligations": round(float(rng.uniform(0, 400_000)), 2),
            }
        )
    columns = [
        "pull_id", "pull_date", "product_line", "business_id", "bureau_name",
        "business_credit_score", "personal_credit_score", "years_in_business", "existing_debt_obligations",
    ]
    return pd.DataFrame(rows, columns=columns)
