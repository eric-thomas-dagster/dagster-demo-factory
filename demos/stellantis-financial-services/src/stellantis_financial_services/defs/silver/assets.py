"""Silver layer: conforming/staging + dimensions, one Fabric-pipeline-triggered
asset per stage.

Each asset still triggers-and-observes a Fabric pipeline (the migrated
SSIS conforming/staging package), exactly like bronze. In demo mode, the SQL
below is what stands in for that pipeline's own stored-procedure logic --
Dagster does not rewrite SFS's transformation logic in a new engine (see
"Explicitly out of scope" in the brief); this is local DuckDB SQL used only
to produce plausible, internally-consistent rows for the demo to show.
"""

import dagster as dg

from stellantis_financial_services.components.fabric_resource import FabricPipelineResource
from stellantis_financial_services.components.partitions import (
    DATE_PARTITIONS_DEF,
    FLOORPLAN_TO_DATE_MAPPING,
)
from stellantis_financial_services.demo_data import generators
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path, upsert_partition

_SILVER_GROUP = "silver"
_SILVER_KINDS = {"fabric", "azure"}


@dg.asset(
    key=dg.AssetKey(["staging", "stg_loan_originations"]),
    deps=[dg.AssetKey(["raw", "raw_loan_originations"])],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_SILVER_GROUP,
    kinds=_SILVER_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description="Typed, conformed loan originations with a risk-tier bucket, from the migrated SSIS staging package.",
)
def stg_loan_originations(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_stg_loan_originations")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select
                loan_id, dealer_id, borrower_id, vehicle_vin, origination_date,
                principal_amount, apr, term_months, channel,
                case when apr < 6.5 then 'prime' when apr < 9.0 then 'near_prime' else 'subprime' end as risk_tier
            from raw.loan_originations
            where origination_date = ?
            """,
            [event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "staging",
            "stg_loan_originations",
            frame,
            match={"origination_date": event_date},
            ddl_columns={
                "loan_id": "VARCHAR",
                "dealer_id": "VARCHAR",
                "borrower_id": "VARCHAR",
                "vehicle_vin": "VARCHAR",
                "origination_date": "VARCHAR",
                "principal_amount": "DOUBLE",
                "apr": "DOUBLE",
                "term_months": "INTEGER",
                "channel": "VARCHAR",
                "risk_tier": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})


@dg.asset(
    key=dg.AssetKey(["staging", "stg_lease_originations"]),
    deps=[dg.AssetKey(["raw", "raw_lease_originations"])],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_SILVER_GROUP,
    kinds=_SILVER_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description="Typed, conformed lease originations, from the migrated SSIS staging package.",
)
def stg_lease_originations(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_stg_lease_originations")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select
                lease_id, dealer_id, borrower_id, vehicle_vin, origination_date,
                residual_value, monthly_payment, term_months, channel,
                round(residual_value / nullif(monthly_payment * term_months, 0), 4) as residual_ratio
            from raw.lease_originations
            where origination_date = ?
            """,
            [event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "staging",
            "stg_lease_originations",
            frame,
            match={"origination_date": event_date},
            ddl_columns={
                "lease_id": "VARCHAR",
                "dealer_id": "VARCHAR",
                "borrower_id": "VARCHAR",
                "vehicle_vin": "VARCHAR",
                "origination_date": "VARCHAR",
                "residual_value": "DOUBLE",
                "monthly_payment": "DOUBLE",
                "term_months": "INTEGER",
                "channel": "VARCHAR",
                "residual_ratio": "DOUBLE",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})


@dg.asset(
    key=dg.AssetKey(["staging", "stg_payment_transactions"]),
    deps=[dg.AssetKey(["raw", "raw_payment_transactions"])],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_SILVER_GROUP,
    kinds=_SILVER_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description="Typed, conformed payment transactions, from the migrated SSIS staging package.",
)
def stg_payment_transactions(
    context: dg.AssetExecutionContext, fabric: FabricPipelineResource
) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_stg_payment_transactions")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select transaction_id, contract_id, contract_type, payment_date, amount, payment_method, servicer
            from raw.payment_transactions
            where payment_date = ?
            """,
            [event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "staging",
            "stg_payment_transactions",
            frame,
            match={"payment_date": event_date},
            ddl_columns={
                "transaction_id": "VARCHAR",
                "contract_id": "VARCHAR",
                "contract_type": "VARCHAR",
                "payment_date": "VARCHAR",
                "amount": "DOUBLE",
                "payment_method": "VARCHAR",
                "servicer": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})


@dg.asset(
    key=dg.AssetKey(["staging", "stg_delinquency_events"]),
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_SILVER_GROUP,
    kinds=_SILVER_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description=(
        "Delinquency events for the day, as already computed by the migrated SSIS "
        "delinquency-detection package -- Dagster observes this output, it does not "
        "recompute the payment-vs-due-date logic behind it."
    ),
)
def stg_delinquency_events(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_stg_delinquency_events")
    frame = generators.generate_delinquency_events(event_date)

    conn = connect_with_retry(demo_duckdb_path())
    try:
        upsert_partition(
            conn,
            "staging",
            "stg_delinquency_events",
            frame,
            match={"event_date": event_date},
            ddl_columns={
                "event_id": "VARCHAR",
                "contract_id": "VARCHAR",
                "dealer_id": "VARCHAR",
                "days_past_due": "INTEGER",
                "delinquency_amount": "DOUBLE",
                "event_date": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})


@dg.asset(
    key=dg.AssetKey(["staging", "dim_dealer"]),
    deps=[
        dg.AssetDep(
            dg.AssetKey(["raw", "raw_dealer_floorplan_feed"]),
            partition_mapping=FLOORPLAN_TO_DATE_MAPPING,
        )
    ],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_SILVER_GROUP,
    kinds=_SILVER_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description=(
        "Daily dealer rollup -- floorplan advance activity per dealer, across all four "
        "regional dealer-group batches for the day. Depends on every `dealer_group` "
        "partition of `raw_dealer_floorplan_feed` via a multi-to-single-dimension "
        "partition mapping."
    ),
)
def dim_dealer(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_dim_dealer")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            select
                dealer_id, dealer_group, ? as as_of_date,
                count(*) as open_floorplan_advances_count,
                sum(advance_amount) as total_floorplan_outstanding_amount
            from raw.dealer_floorplan_feed
            where advance_date = ? and dealer_id is not null
            group by 1, 2
            """,
            [event_date, event_date],
        ).fetchdf()
        frame = frame.merge(generators.dealer_roster()[["dealer_id", "dealer_name"]], on="dealer_id", how="left")
        upsert_partition(
            conn,
            "staging",
            "dim_dealer",
            frame,
            match={"as_of_date": event_date},
            ddl_columns={
                "dealer_id": "VARCHAR",
                "dealer_group": "VARCHAR",
                "as_of_date": "VARCHAR",
                "open_floorplan_advances_count": "BIGINT",
                "total_floorplan_outstanding_amount": "DOUBLE",
                "dealer_name": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})


@dg.asset(
    key=dg.AssetKey(["staging", "dim_borrower"]),
    deps=[
        dg.AssetKey(["raw", "raw_loan_originations"]),
        dg.AssetKey(["raw", "raw_lease_originations"]),
        dg.AssetKey(["raw", "raw_credit_bureau_pull"]),
    ],
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_SILVER_GROUP,
    kinds=_SILVER_KINDS,
    automation_condition=dg.AutomationCondition.eager(),
    description="Daily borrower profile -- origination + credit-bureau context for each new borrower.",
)
def dim_borrower(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_dim_borrower")

    conn = connect_with_retry(demo_duckdb_path())
    try:
        frame = conn.execute(
            """
            with originations as (
                select borrower_id, 'loan' as origination_type, principal_amount as origination_amount, dealer_id
                from raw.loan_originations where origination_date = ?
                union all
                select borrower_id, 'lease' as origination_type, residual_value as origination_amount, dealer_id
                from raw.lease_originations where origination_date = ?
            )
            select
                o.borrower_id, o.origination_type, o.origination_amount, o.dealer_id,
                b.bureau, b.credit_score
            from originations o
            left join raw.credit_bureau_pull b using (borrower_id)
            """,
            [event_date, event_date],
        ).fetchdf()
        upsert_partition(
            conn,
            "staging",
            "dim_borrower",
            frame.assign(as_of_date=event_date),
            match={"as_of_date": event_date},
            ddl_columns={
                "borrower_id": "VARCHAR",
                "origination_type": "VARCHAR",
                "origination_amount": "DOUBLE",
                "dealer_id": "VARCHAR",
                "bureau": "VARCHAR",
                "credit_score": "INTEGER",
                "as_of_date": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]})
