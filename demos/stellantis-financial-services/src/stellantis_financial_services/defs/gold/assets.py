"""Gold layer: loan-level tape and portfolio marts, one Fabric-pipeline-triggered
asset per stage.

`abs_pool_eligibility` is the money-shot terminal asset -- the audit-ready
loan-level tape SFS's ABS securitization calendar depends on. Everything
upstream of it exists so this one asset can be trusted.
"""

from datetime import timedelta

import dagster as dg

from stellantis_financial_services.components.fabric_resource import FabricPipelineResource
from stellantis_financial_services.components.partitions import DATE_PARTITIONS_DEF
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path, upsert_partition

_GOLD_GROUP = "gold"
_GOLD_KINDS = {"fabric", "azure"}


@dg.asset(
    key=dg.AssetKey(["marts", "fact_loan_portfolio"]),
    deps=[
        dg.AssetKey(["staging", "stg_loan_originations"]),
        dg.AssetKey(["staging", "stg_lease_originations"]),
        dg.AssetKey(["staging", "dim_borrower"]),
        dg.AssetKey(["staging", "dim_dealer"]),
    ],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_GOLD_GROUP,
    kinds=_GOLD_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description=(
        "The loan-level data tape: one row per contract (loan or lease) originated that day, "
        "with borrower credit context and dealer region. Everything downstream in gold reads "
        "from this."
    ),
)
def fact_loan_portfolio(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_fact_loan_portfolio")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            with contracts as (
                select loan_id as contract_id, 'loan' as contract_type, dealer_id, borrower_id,
                       origination_date, principal_amount as amount, apr, term_months, channel, risk_tier
                from staging.stg_loan_originations where origination_date = ?
                union all
                select lease_id as contract_id, 'lease' as contract_type, dealer_id, borrower_id,
                       origination_date, residual_value as amount, null as apr, term_months, channel,
                       null as risk_tier
                from staging.stg_lease_originations where origination_date = ?
            )
            select
                c.*, d.dealer_group, d.dealer_name, b.credit_score
            from contracts c
            left join (select distinct dealer_id, dealer_group, dealer_name from staging.dim_dealer) d
                using (dealer_id)
            left join staging.dim_borrower b using (borrower_id)
            """,
            [event_date, event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "marts",
            "fact_loan_portfolio",
            frame,
            match={"origination_date": event_date},
            ddl_columns={
                "contract_id": "VARCHAR",
                "contract_type": "VARCHAR",
                "dealer_id": "VARCHAR",
                "borrower_id": "VARCHAR",
                "origination_date": "VARCHAR",
                "amount": "DOUBLE",
                "apr": "DOUBLE",
                "term_months": "INTEGER",
                "channel": "VARCHAR",
                "risk_tier": "VARCHAR",
                "dealer_group": "VARCHAR",
                "dealer_name": "VARCHAR",
                "credit_score": "INTEGER",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "fabric_run_status": run["status"],
            "total_originated": dg.MetadataValue.text(f"${frame['amount'].sum():,.0f}"),
        }
    )


@dg.asset(
    key=dg.AssetKey(["marts", "fact_delinquency_snapshot"]),
    deps=[
        dg.AssetKey(["staging", "stg_delinquency_events"]),
        dg.AssetKey(["marts", "fact_loan_portfolio"]),
    ],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_GOLD_GROUP,
    kinds=_GOLD_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    freshness_policy=dg.FreshnessPolicy.cron(
        deadline_cron="0 9 * * *", lower_bound_delta=timedelta(hours=2), timezone="America/Detroit"
    ),
    description=(
        "Daily delinquency snapshot with dealer context -- the asset someone at SFS gets paged "
        "over. Freshness policy: must materialize by 9am ET, the answer to 'how would we know "
        "something broke.'"
    ),
)
def fact_delinquency_snapshot(
    context: dg.AssetExecutionContext, fabric: FabricPipelineResource
) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_fact_delinquency_snapshot")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select
                e.event_id, e.contract_id, e.dealer_id, d.dealer_group,
                e.days_past_due, e.delinquency_amount, e.event_date
            from staging.stg_delinquency_events e
            left join (select distinct dealer_id, dealer_group from staging.dim_dealer) d using (dealer_id)
            where e.event_date = ?
            """,
            [event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "marts",
            "fact_delinquency_snapshot",
            frame,
            match={"event_date": event_date},
            ddl_columns={
                "event_id": "VARCHAR",
                "contract_id": "VARCHAR",
                "dealer_id": "VARCHAR",
                "dealer_group": "VARCHAR",
                "days_past_due": "INTEGER",
                "delinquency_amount": "DOUBLE",
                "event_date": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})


@dg.asset(
    key=dg.AssetKey(["marts", "abs_pool_eligibility"]),
    deps=[dg.AssetKey(["marts", "fact_loan_portfolio"]), dg.AssetKey(["marts", "fact_delinquency_snapshot"])],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_GOLD_GROUP,
    kinds=_GOLD_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    freshness_policy=dg.FreshnessPolicy.cron(
        deadline_cron="0 9 * * *", lower_bound_delta=timedelta(hours=2), timezone="America/Detroit"
    ),
    description=(
        "The money-shot asset: per-contract ABS pool eligibility for SFS's 2026 securitization "
        "calendar (up to eight deals this year). Gated by `abs_pool_eligibility_reconciliation` "
        "-- a blocking check -- before anything downstream can call this pool trustworthy."
    ),
)
def abs_pool_eligibility(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_abs_pool_eligibility")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select
                p.contract_id, p.contract_type, p.dealer_id, p.dealer_group, p.amount, p.credit_score,
                coalesce(max(d.days_past_due), 0) as worst_days_past_due,
                (p.credit_score is not null and p.credit_score >= 620
                    and coalesce(max(d.days_past_due), 0) < 60) as is_pool_eligible,
                p.origination_date
            from marts.fact_loan_portfolio p
            left join marts.fact_delinquency_snapshot d
                on d.contract_id = p.contract_id and d.event_date = p.origination_date
            where p.origination_date = ?
            group by p.contract_id, p.contract_type, p.dealer_id, p.dealer_group, p.amount,
                     p.credit_score, p.origination_date
            """,
            [event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "marts",
            "abs_pool_eligibility",
            frame,
            match={"origination_date": event_date},
            ddl_columns={
                "contract_id": "VARCHAR",
                "contract_type": "VARCHAR",
                "dealer_id": "VARCHAR",
                "dealer_group": "VARCHAR",
                "amount": "DOUBLE",
                "credit_score": "INTEGER",
                "worst_days_past_due": "INTEGER",
                "is_pool_eligible": "BOOLEAN",
                "origination_date": "VARCHAR",
            },
        )
    finally:
        conn.close()

    eligible = int(frame["is_pool_eligible"].sum()) if len(frame) else 0
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "fabric_run_status": run["status"],
            "eligible_contracts": eligible,
            "eligible_pct": dg.MetadataValue.text(f"{(eligible / len(frame) * 100) if len(frame) else 0:.1f}%"),
        }
    )


@dg.asset(
    key=dg.AssetKey(["marts", "gl_reconciliation_summary"]),
    deps=[dg.AssetKey(["staging", "stg_payment_transactions"]), dg.AssetKey(["marts", "fact_loan_portfolio"])],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_GOLD_GROUP,
    kinds=_GOLD_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description="Daily payments-received-by-servicer summary, for GL reconciliation against the booked portfolio.",
)
def gl_reconciliation_summary(
    context: dg.AssetExecutionContext, fabric: FabricPipelineResource
) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_gl_reconciliation_summary")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select
                servicer, ? as as_of_date,
                count(*) as transaction_count,
                sum(amount) as total_collected
            from staging.stg_payment_transactions
            where payment_date = ?
            group by 1
            """,
            [event_date, event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "marts",
            "gl_reconciliation_summary",
            frame,
            match={"as_of_date": event_date},
            ddl_columns={
                "servicer": "VARCHAR",
                "as_of_date": "VARCHAR",
                "transaction_count": "BIGINT",
                "total_collected": "DOUBLE",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})


@dg.asset(
    key=dg.AssetKey(["marts", "customer_360"]),
    deps=[
        dg.AssetKey(["staging", "dim_borrower"]),
        dg.AssetKey(["marts", "fact_loan_portfolio"]),
        dg.AssetKey(["marts", "fact_delinquency_snapshot"]),
    ],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_GOLD_GROUP,
    kinds=_GOLD_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description="Combined per-borrower profile -- exposure, credit context, and worst delinquency status.",
)
def customer_360(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_customer_360")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select
                b.borrower_id, b.credit_score, b.bureau,
                count(p.contract_id) as active_contracts_count,
                sum(p.amount) as total_exposure,
                coalesce(max(d.days_past_due), 0) as worst_days_past_due
            from staging.dim_borrower b
            left join marts.fact_loan_portfolio p
                on p.borrower_id = b.borrower_id and p.origination_date = b.as_of_date
            left join marts.fact_delinquency_snapshot d
                on d.contract_id = p.contract_id and d.event_date = b.as_of_date
            where b.as_of_date = ?
            group by b.borrower_id, b.credit_score, b.bureau
            """,
            [event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "marts",
            "customer_360",
            frame.assign(as_of_date=event_date),
            match={"as_of_date": event_date},
            ddl_columns={
                "borrower_id": "VARCHAR",
                "credit_score": "INTEGER",
                "bureau": "VARCHAR",
                "active_contracts_count": "BIGINT",
                "total_exposure": "DOUBLE",
                "worst_days_past_due": "INTEGER",
                "as_of_date": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})
