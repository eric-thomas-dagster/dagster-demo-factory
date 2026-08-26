"""Silver/gold transform SQL.

Stands in for the SSIS/stored-procedure conforming and mart logic SFS is
migrating into Fabric pipelines -- Dagster triggers and gates these steps, it
does not replace their logic (see the brief's "Explicitly out of scope"). Each
function is one Fabric-pipeline-triggered asset's transform, expressed as a
single SQL statement over tables an earlier layer already landed in DuckDB
(standing in for the Fabric lakehouse). All functions are pure functions of
already-landed data -- rerunning one is idempotent, which is the entire
targeted-recovery story.

Every function returns the row count written for the partition, so the
calling asset can report `dagster/row_count` and `validate_e2e.py` can print
it for the determinism check.
"""

from stellantis_financial_services.demo_data.generators import ACCOUNT_POOL_SIZE, DEALER_GROUPS
from stellantis_financial_services.demo_data.warehouse import upsert_via_select

# Delinquency rate for pool eligibility is measured against the serviced back
# book (the population payments are drawn from), not the day's new
# originations -- the two are different populations, and comparing delinquent
# counts to same-day origination counts would produce a meaningless ratio.
_SERVICED_ACCOUNTS_PER_REGION = ACCOUNT_POOL_SIZE // len(DEALER_GROUPS)


def stg_loan_originations(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT DISTINCT loan_id, origination_date, dealer_id, dealer_group,
               borrower_id, vehicle_model, principal_amount, apr, term_months, state
        FROM raw.raw_loan_originations
        WHERE origination_date = '{event_date}'
    """
    return upsert_via_select(conn, "staging", "stg_loan_originations", select_sql, {"origination_date": event_date})


def stg_lease_originations(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT DISTINCT lease_id, origination_date, dealer_id, dealer_group,
               borrower_id, vehicle_model, capitalized_cost, residual_value,
               money_factor, term_months, state
        FROM raw.raw_lease_originations
        WHERE origination_date = '{event_date}'
    """
    return upsert_via_select(conn, "staging", "stg_lease_originations", select_sql, {"origination_date": event_date})


def stg_payment_transactions(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT payment_id, account_id, dealer_group, payment_date, amount_due, amount_paid, days_past_due,
               CASE
                   WHEN days_past_due = 0 THEN 'current'
                   WHEN days_past_due <= 30 THEN 'bucket_30'
                   WHEN days_past_due <= 60 THEN 'bucket_60'
                   ELSE 'bucket_90_plus'
               END AS delinquency_bucket
        FROM raw.raw_payment_transactions
        WHERE payment_date = '{event_date}'
    """
    return upsert_via_select(conn, "staging", "stg_payment_transactions", select_sql, {"payment_date": event_date})


def stg_delinquency_events(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT payment_id, account_id, dealer_group, payment_date AS event_date,
               days_past_due, delinquency_bucket, amount_due - amount_paid AS past_due_amount
        FROM staging.stg_payment_transactions
        WHERE payment_date = '{event_date}' AND days_past_due > 0
    """
    return upsert_via_select(conn, "staging", "stg_delinquency_events", select_sql, {"event_date": event_date})


def dim_dealer(conn, event_date: str) -> int:
    """Rolls up all four `dealer_group` partitions of the floorplan feed for one date."""
    select_sql = f"""
        SELECT dealer_id, dealer_group, feed_date AS as_of_date, units_floored,
               floorplan_balance, curtailment_due_amount, arrival_hour
        FROM raw.raw_dealer_floorplan_feed
        WHERE feed_date = '{event_date}'
    """
    return upsert_via_select(conn, "staging", "dim_dealer", select_sql, {"as_of_date": event_date})


def dim_borrower(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT l.borrower_id, l.dealer_group, 'loan' AS product_type,
               l.principal_amount AS original_amount, l.apr, c.credit_score,
               '{event_date}' AS as_of_date
        FROM staging.stg_loan_originations l
        JOIN raw.raw_credit_bureau_pull c ON c.borrower_id = l.borrower_id AND c.pull_date = '{event_date}'
        WHERE l.origination_date = '{event_date}'
        UNION ALL
        SELECT s.borrower_id, s.dealer_group, 'lease' AS product_type,
               s.capitalized_cost AS original_amount, CAST(NULL AS DOUBLE) AS apr, c.credit_score,
               '{event_date}' AS as_of_date
        FROM staging.stg_lease_originations s
        JOIN raw.raw_credit_bureau_pull c ON c.borrower_id = s.borrower_id AND c.pull_date = '{event_date}'
        WHERE s.origination_date = '{event_date}'
    """
    return upsert_via_select(conn, "staging", "dim_borrower", select_sql, {"as_of_date": event_date})


def fact_loan_portfolio(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT as_of_date, dealer_group, product_type,
               count(*) AS contract_count,
               sum(original_amount) AS total_balance,
               avg(apr) AS avg_apr
        FROM staging.dim_borrower
        WHERE as_of_date = '{event_date}'
        GROUP BY as_of_date, dealer_group, product_type
    """
    return upsert_via_select(conn, "marts", "fact_loan_portfolio", select_sql, {"as_of_date": event_date})


def fact_delinquency_snapshot(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT '{event_date}' AS as_of_date, dealer_group,
               count(*) AS delinquent_accounts,
               sum(past_due_amount) AS total_past_due,
               count(*) FILTER (WHERE delinquency_bucket = 'bucket_90_plus') AS accounts_90_plus_dpd
        FROM staging.stg_delinquency_events
        WHERE event_date = '{event_date}'
        GROUP BY dealer_group
    """
    return upsert_via_select(conn, "marts", "fact_delinquency_snapshot", select_sql, {"as_of_date": event_date})


def abs_pool_eligibility(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT p.as_of_date, p.dealer_group,
               p.contract_count, p.total_balance,
               COALESCE(d.delinquent_accounts, 0) AS delinquent_accounts,
               COALESCE(d.total_past_due, 0.0) AS total_past_due,
               ROUND(COALESCE(d.delinquent_accounts, 0) * 1.0 / {_SERVICED_ACCOUNTS_PER_REGION}, 4) AS delinquency_rate,
               p.total_balance - COALESCE(d.total_past_due, 0.0) AS eligible_balance,
               (COALESCE(d.delinquent_accounts, 0) * 1.0 / {_SERVICED_ACCOUNTS_PER_REGION}) < 0.20 AS pool_eligible
        FROM (
            SELECT as_of_date, dealer_group, sum(contract_count) AS contract_count, sum(total_balance) AS total_balance
            FROM marts.fact_loan_portfolio
            WHERE as_of_date = '{event_date}'
            GROUP BY as_of_date, dealer_group
        ) p
        LEFT JOIN marts.fact_delinquency_snapshot d
          ON d.as_of_date = p.as_of_date AND d.dealer_group = p.dealer_group
    """
    return upsert_via_select(conn, "marts", "abs_pool_eligibility", select_sql, {"as_of_date": event_date})


def gl_reconciliation_summary(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT '{event_date}' AS as_of_date, dealer_group,
               sum(amount_paid) AS total_collected,
               sum(amount_due) AS total_due,
               sum(amount_due) - sum(amount_paid) AS variance
        FROM staging.stg_payment_transactions
        WHERE payment_date = '{event_date}'
        GROUP BY dealer_group
    """
    return upsert_via_select(conn, "marts", "gl_reconciliation_summary", select_sql, {"as_of_date": event_date})


def customer_360(conn, event_date: str) -> int:
    select_sql = f"""
        SELECT b.borrower_id, b.dealer_group, b.product_type, b.original_amount,
               b.apr, b.credit_score, b.as_of_date
        FROM staging.dim_borrower b
        WHERE b.as_of_date = '{event_date}'
    """
    return upsert_via_select(conn, "marts", "customer_360", select_sql, {"as_of_date": event_date})


def refresh_dashboard(conn, event_date: str) -> int:
    """Represents the Power BI refresh-trigger asset -- logs the row counts
    the exec dashboard would pull from the gold layer, rather than rendering
    an actual report (out of scope per the brief).
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS reporting")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS reporting.dashboard_refresh_log "
        "(as_of_date VARCHAR, portfolio_rows BIGINT, delinquency_rows BIGINT, "
        "abs_pool_rows BIGINT, gl_rows BIGINT, customer_360_rows BIGINT)"
    )
    conn.execute("DELETE FROM reporting.dashboard_refresh_log WHERE as_of_date = ?", [event_date])
    counts = conn.execute(
        """
        SELECT
            (SELECT count(*) FROM marts.fact_loan_portfolio WHERE as_of_date = ?),
            (SELECT count(*) FROM marts.fact_delinquency_snapshot WHERE as_of_date = ?),
            (SELECT count(*) FROM marts.abs_pool_eligibility WHERE as_of_date = ?),
            (SELECT count(*) FROM marts.gl_reconciliation_summary WHERE as_of_date = ?),
            (SELECT count(*) FROM marts.customer_360 WHERE as_of_date = ?)
        """,
        [event_date] * 5,
    ).fetchone()
    conn.execute(
        "INSERT INTO reporting.dashboard_refresh_log VALUES (?, ?, ?, ?, ?, ?)",
        [event_date, *counts],
    )
    return sum(counts)
