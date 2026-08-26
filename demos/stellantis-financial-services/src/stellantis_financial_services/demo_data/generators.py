"""Deterministic synthetic generators, one per asset, standing in for the
data a triggered Fabric pipeline would have written to the lakehouse.

Every function is pure in `(date[, dealer_group])` given the module-level
seed baked into each defs.yaml (`demo_seed`) -- same partition, same numbers,
every run. Row counts and cardinalities target a mid-size captive auto
lender (per the brief: hundreds of dealers, thousands of contracts/day),
illustrative rather than sourced since SFS gave no real volumes.

Silver-layer generators deliberately re-derive from their bronze
counterpart's own generator (same seed, same partition) rather than drawing
independent random numbers, so a bronze-to-silver row-count reconciliation
check is exact by construction -- not a coincidence a real check has to get
lucky on.
"""

from __future__ import annotations

import pandas as pd

from stellantis_financial_services.demo_data.reference import (
    all_dealers,
    dealer_roster,
    portfolio_avg_balance,
    portfolio_total_balance,
    portfolio_total_contracts,
    rng,
)

_VEHICLE_TYPES = ["new", "used"]
_BUREAUS = ["Equifax", "Experian", "TransUnion"]
_PAYMENT_METHODS = ["ACH", "card", "check", "portal"]
_DPD_BUCKETS = ["30", "60", "90+"]


# ---------------------------------------------------------------------- bronze


def gen_raw_loan_originations(date: str, seed: int) -> pd.DataFrame:
    """Stands in for the loan-origination Fabric pipeline's landed output."""
    generator = rng(seed, "raw_loan_originations", date)
    n = int(generator.integers(900, 1_400))
    dealers = all_dealers()
    dealer_idx = generator.integers(0, len(dealers), size=n)
    return pd.DataFrame(
        {
            "loan_id": [f"LN-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "origination_date": date,
            "dealer_id": [dealers[i]["dealer_id"] for i in dealer_idx],
            "dealer_group": [dealers[i]["dealer_group"] for i in dealer_idx],
            "borrower_id": [f"BOR-{b:07d}" for b in generator.integers(1_000_000, 9_999_999, size=n)],
            "amount": generator.normal(28_000, 9_000, size=n).clip(8_000, 65_000).round(2),
            "term_months": generator.choice([36, 48, 60, 66, 72], size=n),
            "apr": generator.uniform(4.9, 11.9, size=n).round(2),
            "vehicle_type": generator.choice(_VEHICLE_TYPES, size=n, p=[0.62, 0.38]),
        }
    )


def gen_raw_lease_originations(date: str, seed: int) -> pd.DataFrame:
    generator = rng(seed, "raw_lease_originations", date)
    n = int(generator.integers(250, 500))
    dealers = all_dealers()
    dealer_idx = generator.integers(0, len(dealers), size=n)
    msrp = generator.normal(41_000, 7_500, size=n).clip(22_000, 85_000).round(2)
    residual_pct = generator.uniform(0.48, 0.62, size=n)
    return pd.DataFrame(
        {
            "lease_id": [f"LS-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "origination_date": date,
            "dealer_id": [dealers[i]["dealer_id"] for i in dealer_idx],
            "dealer_group": [dealers[i]["dealer_group"] for i in dealer_idx],
            "borrower_id": [f"BOR-{b:07d}" for b in generator.integers(1_000_000, 9_999_999, size=n)],
            "msrp": msrp,
            "residual_value": (msrp * residual_pct).round(2),
            "money_factor": generator.uniform(0.00125, 0.0021, size=n).round(6),
            "term_months": generator.choice([24, 36, 39], size=n),
            "vehicle_type": "new",
        }
    )


def gen_raw_payment_transactions(date: str, seed: int) -> pd.DataFrame:
    generator = rng(seed, "raw_payment_transactions", date)
    n = int(generator.integers(18_000, 24_000))
    return pd.DataFrame(
        {
            "payment_id": [f"PMT-{date.replace('-', '')}-{i:06d}" for i in range(n)],
            "transaction_date": date,
            "contract_id": [f"CN-{c:06d}" for c in generator.integers(1, portfolio_total_contracts(date), size=n)],
            "amount": generator.normal(465, 140, size=n).clip(35, 2_400).round(2),
            "payment_method": generator.choice(_PAYMENT_METHODS, size=n, p=[0.58, 0.22, 0.11, 0.09]),
            "status": generator.choice(["posted", "pending"], size=n, p=[0.97, 0.03]),
        }
    )


def gen_raw_dealer_floorplan_feed(date: str, dealer_group: str, seed: int) -> pd.DataFrame:
    dealers = dealer_roster(dealer_group, seed)
    generator = rng(seed, "raw_dealer_floorplan_feed", date, dealer_group)
    n = len(dealers)
    return pd.DataFrame(
        {
            "dealer_id": [d["dealer_id"] for d in dealers],
            "dealer_group": dealer_group,
            "as_of_date": date,
            "units_financed": generator.integers(8, 240, size=n),
            "floorplan_balance": generator.uniform(180_000, 3_600_000, size=n).round(2),
            "curtailment_due": generator.uniform(0, 45_000, size=n).round(2),
            "feed_received_at": f"{date}T09:40:00",
        }
    )


def gen_raw_credit_bureau_pull(date: str, seed: int) -> pd.DataFrame:
    generator = rng(seed, "raw_credit_bureau_pull", date)
    n = int(generator.integers(1_200, 1_900))
    return pd.DataFrame(
        {
            "pull_id": [f"CB-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "pull_date": date,
            "applicant_id": [f"APP-{a:07d}" for a in generator.integers(1_000_000, 9_999_999, size=n)],
            "bureau_name": generator.choice(_BUREAUS, size=n),
            "credit_score": generator.normal(701, 55, size=n).clip(520, 850).round(0).astype(int),
            "pull_type": generator.choice(["initial", "re_pull"], size=n, p=[0.83, 0.17]),
        }
    )


# ---------------------------------------------------------------------- silver


def gen_stg_loan_originations(date: str, seed: int) -> pd.DataFrame:
    """Conformed loan originations. Same grain as bronze (1 row per loan) by
    construction, so bronze-to-silver row counts always reconcile."""
    frame = gen_raw_loan_originations(date, seed)
    frame["is_used_vehicle"] = frame["vehicle_type"] == "used"
    frame["ltv_ratio"] = (frame["amount"] / (frame["amount"] * 1.08)).round(4)
    return frame


def gen_stg_lease_originations(date: str, seed: int) -> pd.DataFrame:
    frame = gen_raw_lease_originations(date, seed)
    frame["residual_pct"] = (frame["residual_value"] / frame["msrp"]).round(4)
    return frame


def gen_stg_payment_transactions(date: str, seed: int) -> pd.DataFrame:
    frame = gen_raw_payment_transactions(date, seed)
    frame["payment_month"] = date[:7]
    return frame


def gen_stg_delinquency_events(date: str, seed: int) -> pd.DataFrame:
    """Delinquency events derived from the day's servicing activity -- no
    bronze counterpart, this is where the Fabric pipeline evaluates
    payment history against the servicing calendar."""
    generator = rng(seed, "stg_delinquency_events", date)
    n = int(generator.integers(150, 400))
    return pd.DataFrame(
        {
            "event_id": [f"DLQ-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "event_date": date,
            "contract_id": [f"CN-{c:06d}" for c in generator.integers(1, portfolio_total_contracts(date), size=n)],
            "dpd_bucket": generator.choice(_DPD_BUCKETS, size=n, p=[0.62, 0.27, 0.11]),
            "amount_past_due": generator.uniform(80, 2_600, size=n).round(2),
        }
    )


def gen_dim_dealer(date: str, seed: int) -> pd.DataFrame:  # noqa: ARG001 - seed kept for signature symmetry
    """Dealer dimension: one row per dealer, rolled up across all four
    regional floorplan feeds for the date (mirrors the
    `MultiToSingleDimensionPartitionMapping` dependency this asset carries)."""
    frame = pd.DataFrame(all_dealers())
    frame["as_of_date"] = date
    return frame


def gen_dim_borrower(date: str, seed: int) -> pd.DataFrame:
    generator = rng(seed, "dim_borrower", date)
    n = int(generator.integers(200, 500))
    tiers = ["prime", "near_prime", "subprime"]
    return pd.DataFrame(
        {
            "borrower_id": [f"BOR-{b:07d}" for b in generator.integers(1_000_000, 9_999_999, size=n)],
            "as_of_date": date,
            "state": generator.choice(["MI", "OH", "TX", "CA", "NY", "FL", "GA", "IL"], size=n),
            "credit_tier": generator.choice(tiers, size=n, p=[0.58, 0.30, 0.12]),
        }
    )


# ---------------------------------------------------------------------- gold


def gen_fact_loan_portfolio(date: str, seed: int) -> pd.DataFrame:
    """Point-in-time active servicing book -- a snapshot, not today's
    originations, which is why its row count is an order of magnitude above
    the bronze ingestion assets."""
    n = portfolio_total_contracts(date)
    generator = rng(seed, "fact_loan_portfolio", date)
    avg_balance = portfolio_avg_balance(date)
    return pd.DataFrame(
        {
            "contract_id": [f"CN-{c:06d}" for c in range(1, n + 1)],
            "as_of_date": date,
            "product_type": generator.choice(["retail_installment", "lease"], size=n, p=[0.78, 0.22]),
            "dealer_group": generator.choice(["midwest", "northeast", "south", "west"], size=n),
            "outstanding_balance": generator.normal(avg_balance, 6_500, size=n).clip(500, 68_000).round(2),
            "months_on_book": generator.integers(1, 72, size=n),
            "status": generator.choice(["current", "delinquent"], size=n, p=[0.955, 0.045]),
        }
    )


def gen_fact_delinquency_snapshot(date: str, seed: int) -> pd.DataFrame:
    """Delinquent slice of the same portfolio -- freshness-policed, this is
    the table someone gets paged over."""
    portfolio = gen_fact_loan_portfolio(date, seed)
    delinquent = portfolio[portfolio["status"] == "delinquent"].copy()
    generator = rng(seed, "fact_delinquency_snapshot", date)
    delinquent["dpd_bucket"] = generator.choice(_DPD_BUCKETS, size=len(delinquent), p=[0.55, 0.30, 0.15])
    return delinquent[["contract_id", "as_of_date", "dealer_group", "outstanding_balance", "dpd_bucket"]]


def gen_abs_pool_eligibility(date: str, seed: int) -> pd.DataFrame:
    """Securitization-pool-eligible subset: current contracts only, minus a
    small documentation-exception bucket. Eligible rows are a strict subset
    of `fact_loan_portfolio`'s current contracts by construction, so the
    blocking reconciliation check here always passes."""
    portfolio = gen_fact_loan_portfolio(date, seed)
    current = portfolio[portfolio["status"] == "current"].copy()
    generator = rng(seed, "abs_pool_eligibility", date)
    exception_mask = generator.uniform(0, 1, size=len(current)) < 0.018
    eligible = current[~exception_mask].copy()
    eligible["pool_eligible"] = True
    return eligible[["contract_id", "as_of_date", "dealer_group", "outstanding_balance", "product_type", "pool_eligible"]]


def gen_gl_reconciliation_summary(date: str, seed: int) -> pd.DataFrame:
    """GL account rollup. Balances are apportioned from the same
    `portfolio_total_balance` the portfolio snapshot itself sums to, so this
    always reconciles -- computed from the shared reference figure rather
    than by re-reading the portfolio table."""
    generator = rng(seed, "gl_reconciliation_summary", date)
    accounts = [
        ("1400-RETAIL-INSTALLMENT", 0.62),
        ("1410-LEASE-RECEIVABLE", 0.22),
        ("1420-DEALER-FLOORPLAN", 0.11),
        ("1430-ACCRUED-INTEREST", 0.05),
    ]
    total = portfolio_total_balance(date)
    weights = generator.dirichlet([20] * len(accounts))
    return pd.DataFrame(
        {
            "gl_account": [a for a, _ in accounts],
            "as_of_date": date,
            "balance": [round(total * base * (1 + (w - 1 / len(accounts)) * 0.05), 2) for (_, base), w in zip(accounts, weights)],
        }
    )


def gen_customer_360(date: str, seed: int) -> pd.DataFrame:
    """Borrower-level rollup for the Customer Data Platform team's view --
    same borrower slice as `dim_borrower` (same seed/date) plus
    portfolio-relationship fields."""
    borrowers = gen_dim_borrower(date, seed)
    generator = rng(seed, "customer_360", date)
    borrowers = borrowers.copy()
    borrowers["active_contracts"] = generator.integers(1, 4, size=len(borrowers))
    borrowers["lifetime_originated_amount"] = generator.uniform(9_000, 140_000, size=len(borrowers)).round(2)
    return borrowers


def gen_powerbi_portfolio_dashboard_refresh(date: str, seed: int) -> pd.DataFrame:
    """One row per day representing the Power BI dataset refresh Fabric
    triggers after the gold layer lands -- not a real embedded report."""
    generator = rng(seed, "powerbi_portfolio_dashboard_refresh", date)
    return pd.DataFrame(
        {
            "dataset_name": ["Portfolio Overview"],
            "refresh_date": [date],
            "rows_refreshed": [portfolio_total_contracts(date)],
            "duration_seconds": [int(generator.integers(40, 140))],
            "refresh_status": ["Succeeded"],
        }
    )


GENERATORS = {
    "raw_loan_originations": gen_raw_loan_originations,
    "raw_lease_originations": gen_raw_lease_originations,
    "raw_payment_transactions": gen_raw_payment_transactions,
    "raw_credit_bureau_pull": gen_raw_credit_bureau_pull,
    "stg_loan_originations": gen_stg_loan_originations,
    "stg_lease_originations": gen_stg_lease_originations,
    "stg_payment_transactions": gen_stg_payment_transactions,
    "stg_delinquency_events": gen_stg_delinquency_events,
    "dim_dealer": gen_dim_dealer,
    "dim_borrower": gen_dim_borrower,
    "fact_loan_portfolio": gen_fact_loan_portfolio,
    "fact_delinquency_snapshot": gen_fact_delinquency_snapshot,
    "abs_pool_eligibility": gen_abs_pool_eligibility,
    "gl_reconciliation_summary": gen_gl_reconciliation_summary,
    "customer_360": gen_customer_360,
    "powerbi_portfolio_dashboard_refresh": gen_powerbi_portfolio_dashboard_refresh,
}

# raw_dealer_floorplan_feed takes an extra `dealer_group` argument, so it is
# dispatched separately rather than through the single-date `GENERATORS` map.
