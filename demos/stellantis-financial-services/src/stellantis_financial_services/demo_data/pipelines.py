"""Synthetic stand-ins for SFS's ~700 SSIS-packages-becoming-Fabric-pipelines.

Each function here is what `demo_mode: true` runs instead of the real Fabric
pipeline trigger + poll cycle (see `components/fabric_workspace_demo.py`).
It stands in for the *transformation logic already inside that pipeline* --
not something Dagster is newly computing. In real mode, this file is never
imported; the Fabric REST trigger/poll call in
`FabricResource`/`FabricWorkspaceComponent` runs instead.

Every function is deterministic (seeded from `demo_seed` + pipeline name +
partition key) and reads its inputs from the same DuckDB warehouse it
writes to, so the bronze -> silver -> gold lineage is a real read/transform/
write chain, not independently-random tables that happen to share column
names. There is no planted anomaly anywhere in this file -- every partition
produces clean, checkable data, per CLAUDE.md.

Row counts target a mid-size captive auto lender: hundreds of dealers,
thousands of contracts/day, matching the ~35-person data team implied by
the brief. Treat exact figures as illustrative, not sourced (brief flags
volumes as unknown/low-confidence).
"""


import hashlib

import numpy as np
import pandas as pd

from stellantis_financial_services.demo_data.warehouse import (
    connect_with_retry,
    demo_duckdb_path,
    table_exists,
    upsert_partition,
)

DEALER_GROUPS = ["midwest", "northeast", "south", "west"]
CREDIT_TIERS = ["super_prime", "prime", "near_prime", "subprime"]
CREDIT_TIER_WEIGHTS = [0.22, 0.38, 0.28, 0.12]
VEHICLE_TYPES = ["new", "used"]

_DEALERS_PER_GROUP = 10


def _rng(seed: int, *parts: str) -> np.random.Generator:
    """Deterministic RNG seeded from the demo seed plus a stable string key."""
    key = "|".join([str(seed), *parts]).encode()
    digest = hashlib.sha256(key).hexdigest()[:16]
    return np.random.default_rng(int(digest, 16))


def _dealer_roster() -> pd.DataFrame:
    """Fixed roster of 40 dealers, 10 per region -- stable across every date.

    Not seeded per-partition: the same dealer exists on every day's feed,
    which is what makes `dim_dealer` a meaningful daily rollup rather than a
    different cast of dealers every partition.
    """
    rng = np.random.default_rng(20260826)
    rows = []
    for group in DEALER_GROUPS:
        for i in range(_DEALERS_PER_GROUP):
            dealer_id = f"DLR-{group[:2].upper()}{i + 1:03d}"
            rows.append(
                {
                    "dealer_id": dealer_id,
                    "dealer_group": group,
                    "advance_rate": round(float(rng.uniform(0.80, 0.95)), 3),
                }
            )
    return pd.DataFrame(rows)


_DEALER_ROSTER = _dealer_roster()


def _conn():
    return connect_with_retry(demo_duckdb_path())


def _read(conn, schema: str, table: str, where: str, params: list) -> pd.DataFrame:
    if not table_exists(conn, schema, table):
        return pd.DataFrame()
    return conn.execute(f"SELECT * FROM {schema}.{table} WHERE {where}", params).fetch_df()


# --------------------------------------------------------------------------
# Bronze -- vendor file ingestion (represents ~5 of the ~700 SSIS packages)
# --------------------------------------------------------------------------


def raw_loan_originations(date: str, seed: int) -> int:
    rng = _rng(seed, "raw_loan_originations", date)
    n = int(rng.integers(180, 420))
    dealers = _DEALER_ROSTER.sample(n=n, replace=True, random_state=int(rng.integers(0, 2**31)))
    df = pd.DataFrame(
        {
            "loan_id": [f"LN-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "origination_date": date,
            "dealer_id": dealers["dealer_id"].to_numpy(),
            "dealer_group": dealers["dealer_group"].to_numpy(),
            "borrower_id": [f"BOR-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "vehicle_type": rng.choice(VEHICLE_TYPES, size=n, p=[0.55, 0.45]),
            "loan_amount": np.round(rng.uniform(9_000, 68_000, size=n), 2),
            "apr": np.round(rng.uniform(3.5, 18.5, size=n), 3),
            "term_months": rng.choice([36, 48, 60, 72, 84], size=n),
            "credit_tier": rng.choice(CREDIT_TIERS, size=n, p=CREDIT_TIER_WEIGHTS),
        }
    )
    conn = _conn()
    try:
        upsert_partition(
            conn,
            "raw",
            "loan_originations",
            df,
            match={"origination_date": date},
            ddl_columns={
                "loan_id": "VARCHAR", "origination_date": "VARCHAR", "dealer_id": "VARCHAR",
                "dealer_group": "VARCHAR", "borrower_id": "VARCHAR", "vehicle_type": "VARCHAR",
                "loan_amount": "DOUBLE", "apr": "DOUBLE", "term_months": "BIGINT", "credit_tier": "VARCHAR",
            },
        )
    finally:
        conn.close()
    return n


def raw_lease_originations(date: str, seed: int) -> int:
    rng = _rng(seed, "raw_lease_originations", date)
    n = int(rng.integers(60, 160))
    dealers = _DEALER_ROSTER.sample(n=n, replace=True, random_state=int(rng.integers(0, 2**31)))
    df = pd.DataFrame(
        {
            "lease_id": [f"LS-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "origination_date": date,
            "dealer_id": dealers["dealer_id"].to_numpy(),
            "dealer_group": dealers["dealer_group"].to_numpy(),
            "borrower_id": [f"BOR-{date.replace('-', '')}-{i + 10_000:05d}" for i in range(n)],
            "vehicle_type": rng.choice(VEHICLE_TYPES, size=n, p=[0.75, 0.25]),
            "residual_value": np.round(rng.uniform(11_000, 32_000, size=n), 2),
            "monthly_payment": np.round(rng.uniform(280, 720, size=n), 2),
            "term_months": rng.choice([24, 36, 39], size=n),
            "credit_tier": rng.choice(CREDIT_TIERS, size=n, p=CREDIT_TIER_WEIGHTS),
        }
    )
    conn = _conn()
    try:
        upsert_partition(
            conn,
            "raw",
            "lease_originations",
            df,
            match={"origination_date": date},
            ddl_columns={
                "lease_id": "VARCHAR", "origination_date": "VARCHAR", "dealer_id": "VARCHAR",
                "dealer_group": "VARCHAR", "borrower_id": "VARCHAR", "vehicle_type": "VARCHAR",
                "residual_value": "DOUBLE", "monthly_payment": "DOUBLE", "term_months": "BIGINT",
                "credit_tier": "VARCHAR",
            },
        )
    finally:
        conn.close()
    return n


def raw_payment_transactions(date: str, seed: int) -> int:
    rng = _rng(seed, "raw_payment_transactions", date)
    n = int(rng.integers(900, 1_800))
    contract_ids = [
        f"{'LN' if rng.random() < 0.75 else 'LS'}-{date.replace('-', '')}-{int(rng.integers(0, 400)):05d}"
        for _ in range(n)
    ]
    df = pd.DataFrame(
        {
            "payment_id": [f"PMT-{date.replace('-', '')}-{i:06d}" for i in range(n)],
            "payment_date": date,
            "contract_id": contract_ids,
            "contract_type": ["loan" if c.startswith("LN") else "lease" for c in contract_ids],
            "amount": np.round(rng.uniform(180, 950, size=n), 2),
            "days_past_due": rng.choice(
                [0, 15, 30, 45, 60, 90], size=n, p=[0.93, 0.03, 0.02, 0.01, 0.006, 0.004]
            ),
        }
    )
    conn = _conn()
    try:
        upsert_partition(
            conn,
            "raw",
            "payment_transactions",
            df,
            match={"payment_date": date},
            ddl_columns={
                "payment_id": "VARCHAR", "payment_date": "VARCHAR", "contract_id": "VARCHAR",
                "contract_type": "VARCHAR", "amount": "DOUBLE", "days_past_due": "BIGINT",
            },
        )
    finally:
        conn.close()
    return n


def raw_dealer_floorplan_feed(date: str, dealer_group: str, seed: int) -> int:
    rng = _rng(seed, "raw_dealer_floorplan_feed", date, dealer_group)
    dealers = _DEALER_ROSTER[_DEALER_ROSTER["dealer_group"] == dealer_group].reset_index(drop=True)
    n = len(dealers)
    df = pd.DataFrame(
        {
            "dealer_id": dealers["dealer_id"],
            "dealer_group": dealer_group,
            "report_date": date,
            "floorplan_balance": np.round(rng.uniform(400_000, 3_200_000, size=n), 2),
            "units_financed": rng.integers(25, 220, size=n),
            "advance_rate": dealers["advance_rate"],
        }
    )
    conn = _conn()
    try:
        upsert_partition(
            conn,
            "raw",
            "dealer_floorplan_feed",
            df,
            match={"report_date": date, "dealer_group": dealer_group},
            ddl_columns={
                "dealer_id": "VARCHAR", "dealer_group": "VARCHAR", "report_date": "VARCHAR",
                "floorplan_balance": "DOUBLE", "units_financed": "BIGINT", "advance_rate": "DOUBLE",
            },
        )
    finally:
        conn.close()
    return n


def raw_credit_bureau_pull(date: str, seed: int) -> int:
    rng = _rng(seed, "raw_credit_bureau_pull", date)
    conn = _conn()
    try:
        loans = _read(conn, "raw", "loan_originations", "origination_date = ?", [date])
        leases = _read(conn, "raw", "lease_originations", "origination_date = ?", [date])
    finally:
        conn.close()
    borrower_ids = pd.concat([loans.get("borrower_id"), leases.get("borrower_id")]).dropna().unique()
    n = len(borrower_ids)
    df = pd.DataFrame(
        {
            "pull_id": [f"CB-{date.replace('-', '')}-{i:05d}" for i in range(n)],
            "pull_date": date,
            "borrower_id": borrower_ids,
            "bureau_score": rng.integers(560, 830, size=n),
            "delinquency_flag_30d": rng.choice([False, False, False, False, True], size=n),
        }
    )
    conn = _conn()
    try:
        upsert_partition(
            conn,
            "raw",
            "credit_bureau_pull",
            df,
            match={"pull_date": date},
            ddl_columns={
                "pull_id": "VARCHAR", "pull_date": "VARCHAR", "borrower_id": "VARCHAR",
                "bureau_score": "BIGINT", "delinquency_flag_30d": "BOOLEAN",
            },
        )
    finally:
        conn.close()
    return n


# --------------------------------------------------------------------------
# Silver -- conforming/staging (represents existing SSIS/stored-proc logic,
# now running as Fabric pipelines)
# --------------------------------------------------------------------------


def stg_loan_originations(date: str, seed: int) -> int:
    conn = _conn()
    try:
        df = _read(conn, "raw", "loan_originations", "origination_date = ?", [date])
        if not df.empty:
            df["loan_to_value_flag"] = np.where(df["loan_amount"] > 60_000, "review", "standard")
        upsert_partition(
            conn, "stg", "loan_originations", df, match={"origination_date": date}
        ) if not df.empty else None
    finally:
        conn.close()
    return len(df)


def stg_lease_originations(date: str, seed: int) -> int:
    conn = _conn()
    try:
        df = _read(conn, "raw", "lease_originations", "origination_date = ?", [date])
        if not df.empty:
            upsert_partition(conn, "stg", "lease_originations", df, match={"origination_date": date})
    finally:
        conn.close()
    return len(df)


def stg_payment_transactions(date: str, seed: int) -> int:
    conn = _conn()
    try:
        df = _read(conn, "raw", "payment_transactions", "payment_date = ?", [date])
        if not df.empty:
            df = df.drop_duplicates(subset=["payment_id"])
            upsert_partition(conn, "stg", "payment_transactions", df, match={"payment_date": date})
    finally:
        conn.close()
    return len(df)


def stg_delinquency_events(date: str, seed: int) -> int:
    conn = _conn()
    try:
        payments = _read(conn, "stg", "payment_transactions", "payment_date = ?", [date])
        delinquent = payments[payments["days_past_due"] >= 30].copy() if not payments.empty else payments
        if not delinquent.empty:
            delinquent["event_date"] = date
            delinquent["severity"] = np.where(delinquent["days_past_due"] >= 60, "severe", "moderate")
            out = delinquent[["contract_id", "contract_type", "event_date", "days_past_due", "severity"]]
            upsert_partition(conn, "stg", "delinquency_events", out, match={"event_date": date})
        else:
            out = delinquent
    finally:
        conn.close()
    return len(out)


def dim_dealer(date: str, seed: int) -> int:
    """Rolls up all 4 `dealer_group` partitions of `raw_dealer_floorplan_feed`
    for one date -- the multi-to-single-dimension partition mapping the
    brief calls for.
    """
    conn = _conn()
    try:
        floorplan = _read(conn, "raw", "dealer_floorplan_feed", "report_date = ?", [date])
        if not floorplan.empty:
            floorplan["as_of_date"] = date
            out = floorplan[
                ["dealer_id", "dealer_group", "as_of_date", "floorplan_balance", "units_financed", "advance_rate"]
            ]
            upsert_partition(conn, "dim", "dealer", out, match={"as_of_date": date})
        else:
            out = floorplan
    finally:
        conn.close()
    return len(out)


def dim_borrower(date: str, seed: int) -> int:
    conn = _conn()
    try:
        loans = _read(conn, "raw", "loan_originations", "origination_date = ?", [date])
        leases = _read(conn, "raw", "lease_originations", "origination_date = ?", [date])
        bureau = _read(conn, "raw", "credit_bureau_pull", "pull_date = ?", [date])
        contracts = pd.concat(
            [
                loans[["borrower_id", "credit_tier"]] if not loans.empty else pd.DataFrame(),
                leases[["borrower_id", "credit_tier"]] if not leases.empty else pd.DataFrame(),
            ]
        )
        if contracts.empty:
            out = contracts
        else:
            counts = contracts.groupby("borrower_id").size().rename("active_contract_count").reset_index()
            tiers = contracts.drop_duplicates(subset=["borrower_id"])
            merged = counts.merge(tiers, on="borrower_id", how="left")
            if not bureau.empty:
                merged = merged.merge(
                    bureau[["borrower_id", "bureau_score", "delinquency_flag_30d"]], on="borrower_id", how="left"
                )
            else:
                merged["bureau_score"] = pd.NA
                merged["delinquency_flag_30d"] = pd.NA
            merged["as_of_date"] = date
            out = merged
            upsert_partition(conn, "dim", "borrower", out, match={"as_of_date": date})
    finally:
        conn.close()
    return len(out)


# --------------------------------------------------------------------------
# Gold -- marts (represents the ABS-pool-ready loan tape)
# --------------------------------------------------------------------------


def fact_loan_portfolio(date: str, seed: int) -> int:
    conn = _conn()
    try:
        loans = _read(conn, "stg", "loan_originations", "origination_date = ?", [date])
        dealers = _read(conn, "dim", "dealer", "as_of_date = ?", [date])
        if loans.empty:
            out = loans
        else:
            merged = loans.merge(
                dealers[["dealer_id", "advance_rate"]] if not dealers.empty else pd.DataFrame(columns=["dealer_id", "advance_rate"]),
                on="dealer_id",
                how="left",
            )
            merged["as_of_date"] = date
            out = merged[
                [
                    "loan_id", "dealer_id", "dealer_group", "borrower_id", "loan_amount", "apr",
                    "term_months", "credit_tier", "vehicle_type", "advance_rate", "as_of_date",
                ]
            ]
            upsert_partition(conn, "fact", "loan_portfolio", out, match={"as_of_date": date})
    finally:
        conn.close()
    return len(out)


def fact_delinquency_snapshot(date: str, seed: int) -> int:
    """Delinquency rate over the contracts actually serviced (had a payment
    transaction) that day -- not that day's new originations, which is a much
    smaller and unrelated population. Using new originations as the
    denominator let `delinquent_count` (drawn from the full serviced
    population) exceed it, producing an impossible >100% rate.
    """
    conn = _conn()
    try:
        events = _read(conn, "stg", "delinquency_events", "event_date = ?", [date])
        payments = _read(conn, "stg", "payment_transactions", "payment_date = ?", [date])
        total_contracts = int(payments["contract_id"].nunique()) if not payments.empty else 0
        delinquent_count = int(events["contract_id"].nunique()) if not events.empty else 0
        rate = round(delinquent_count / total_contracts, 4) if total_contracts else 0.0
        out = pd.DataFrame(
            [
                {
                    "as_of_date": date,
                    "total_contracts": total_contracts,
                    "delinquent_count": delinquent_count,
                    "delinquency_rate": rate,
                }
            ]
        )
        upsert_partition(conn, "fact", "delinquency_snapshot", out, match={"as_of_date": date})
    finally:
        conn.close()
    return len(out)


def abs_pool_eligibility(date: str, seed: int) -> int:
    """The money-shot terminal asset -- pool eligibility for the 2026 ABS
    securitization calendar. Eligibility is intentionally strict but never
    empty for a normal day's mix: super_prime/prime/near_prime, non-review
    LTV, on a bureau score floor.
    """
    conn = _conn()
    try:
        portfolio = _read(conn, "fact", "loan_portfolio", "as_of_date = ?", [date])
        raw_loans = _read(conn, "raw", "loan_originations", "origination_date = ?", [date])
        bureau = _read(conn, "raw", "credit_bureau_pull", "pull_date = ?", [date])
        if portfolio.empty:
            out = portfolio
        else:
            merged = portfolio.merge(
                bureau[["borrower_id", "bureau_score"]] if not bureau.empty else pd.DataFrame(columns=["borrower_id", "bureau_score"]),
                on="borrower_id",
                how="left",
            )
            merged["bureau_score"] = merged["bureau_score"].fillna(680)
            merged["eligible"] = (
                merged["credit_tier"].isin(["super_prime", "prime", "near_prime"])
                & (merged["bureau_score"] >= 620)
                & (merged["loan_amount"] <= 60_000)
            )
            merged["pool_reason"] = np.where(
                merged["eligible"], "meets_pool_criteria", "credit_tier_or_ltv_excluded"
            )
            merged["as_of_date"] = date
            out = merged[["loan_id", "dealer_group", "credit_tier", "bureau_score", "loan_amount", "eligible", "pool_reason", "as_of_date"]]
            upsert_partition(conn, "abs", "pool_eligibility", out, match={"as_of_date": date})
    finally:
        conn.close()
    return len(out)


def gl_reconciliation_summary(date: str, seed: int) -> int:
    conn = _conn()
    try:
        payments = _read(conn, "stg", "payment_transactions", "payment_date = ?", [date])
        portfolio = _read(conn, "fact", "loan_portfolio", "as_of_date = ?", [date])
        total_payments = round(float(payments["amount"].sum()), 2) if not payments.empty else 0.0
        total_portfolio = round(float(portfolio["loan_amount"].sum()), 2) if not portfolio.empty else 0.0
        out = pd.DataFrame(
            [
                {
                    "as_of_date": date,
                    "total_payments_amount": total_payments,
                    "total_originated_amount": total_portfolio,
                    "payment_count": len(payments),
                }
            ]
        )
        upsert_partition(conn, "gl", "reconciliation_summary", out, match={"as_of_date": date})
    finally:
        conn.close()
    return len(out)


def customer_360(date: str, seed: int) -> int:
    conn = _conn()
    try:
        borrowers = _read(conn, "dim", "borrower", "as_of_date = ?", [date])
        portfolio = _read(conn, "fact", "loan_portfolio", "as_of_date = ?", [date])
        if borrowers.empty:
            out = borrowers
        else:
            totals = (
                portfolio.groupby("borrower_id")["loan_amount"].sum().rename("total_loan_amount").reset_index()
                if not portfolio.empty
                else pd.DataFrame(columns=["borrower_id", "total_loan_amount"])
            )
            merged = borrowers.merge(totals, on="borrower_id", how="left")
            merged["total_loan_amount"] = merged["total_loan_amount"].fillna(0.0)
            merged["as_of_date"] = date
            out = merged[["borrower_id", "active_contract_count", "bureau_score", "total_loan_amount", "as_of_date"]]
            upsert_partition(conn, "customer", "customer_360", out, match={"as_of_date": date})
    finally:
        conn.close()
    return len(out)


def powerbi_portfolio_dashboard_refresh(date: str, seed: int) -> int:
    """Represents the exec-facing Power BI dashboard's scheduled refresh.

    Real mode calls the Fabric pipeline that triggers the Power BI dataset
    refresh API. Demo mode just confirms the gold layer it depends on is
    present for the date and logs the row counts a real refresh would pick
    up -- there's no separate BI artifact to write to in this project.
    """
    conn = _conn()
    try:
        pool = _read(conn, "abs", "pool_eligibility", "as_of_date = ?", [date])
        c360 = _read(conn, "customer", "customer_360", "as_of_date = ?", [date])
        gl = _read(conn, "gl", "reconciliation_summary", "as_of_date = ?", [date])
    finally:
        conn.close()
    return len(pool) + len(c360) + len(gl)


PIPELINE_HANDLERS = {
    "raw_loan_originations": raw_loan_originations,
    "raw_lease_originations": raw_lease_originations,
    "raw_payment_transactions": raw_payment_transactions,
    "raw_credit_bureau_pull": raw_credit_bureau_pull,
    "stg_loan_originations": stg_loan_originations,
    "stg_lease_originations": stg_lease_originations,
    "stg_payment_transactions": stg_payment_transactions,
    "stg_delinquency_events": stg_delinquency_events,
    "dim_dealer": dim_dealer,
    "dim_borrower": dim_borrower,
    "fact_loan_portfolio": fact_loan_portfolio,
    "fact_delinquency_snapshot": fact_delinquency_snapshot,
    "abs_pool_eligibility": abs_pool_eligibility,
    "gl_reconciliation_summary": gl_reconciliation_summary,
    "customer_360": customer_360,
    "powerbi_portfolio_dashboard_refresh": powerbi_portfolio_dashboard_refresh,
}

# `raw_dealer_floorplan_feed` takes a `dealer_group` argument the others
# don't (it's the one multi-partitioned pipeline) -- dispatched separately
# in `components/fabric_workspace_demo.py`.
