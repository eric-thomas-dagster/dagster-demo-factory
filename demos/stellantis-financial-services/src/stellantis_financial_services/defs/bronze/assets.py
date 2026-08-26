"""Bronze layer: vendor-file ingestion, one Fabric-pipeline-triggered asset per feed.

Each asset is a trigger-and-observe wrapper around a Fabric Data Pipeline that
already exists (or is actively being migrated from SSIS) at SFS -- see
`components/fabric_resource.py`. Materializing the asset triggers that
pipeline and lands what it produced; Dagster is not recomputing SFS's
ingestion logic, it is orchestrating and observing it.
"""

import dagster as dg

from stellantis_financial_services.components.fabric_resource import FabricPipelineResource
from stellantis_financial_services.components.partitions import (
    DATE_PARTITIONS_DEF,
    FLOORPLAN_MULTI_PARTITIONS_DEF,
)
from stellantis_financial_services.demo_data import fabric_source_state, generators
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path, upsert_partition

_BRONZE_GROUP = "bronze"
_BRONZE_KINDS = {"fabric", "azure"}


@dg.asset(
    key=dg.AssetKey(["raw", "raw_loan_originations"]),
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_BRONZE_GROUP,
    kinds=_BRONZE_KINDS,
    description=(
        "Daily retail loan originations, landed by the Fabric pipeline migrated from the "
        "legacy SSIS origination-extract package."
    ),
)
def raw_loan_originations(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_raw_loan_originations")
    frame = generators.generate_loan_originations(event_date)

    conn = connect_with_retry(demo_duckdb_path())
    try:
        upsert_partition(
            conn,
            "raw",
            "loan_originations",
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
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "fabric_run_status": run["status"],
            "total_principal": dg.MetadataValue.text(f"${frame['principal_amount'].sum():,.0f}"),
        }
    )


@dg.asset(
    key=dg.AssetKey(["raw", "raw_lease_originations"]),
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_BRONZE_GROUP,
    kinds=_BRONZE_KINDS,
    description=(
        "Daily lease originations, landed by the Fabric pipeline migrated from the legacy "
        "SSIS lease-extract package."
    ),
)
def raw_lease_originations(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_raw_lease_originations")
    frame = generators.generate_lease_originations(event_date)

    conn = connect_with_retry(demo_duckdb_path())
    try:
        upsert_partition(
            conn,
            "raw",
            "lease_originations",
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
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(
        metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]}
    )


@dg.asset(
    key=dg.AssetKey(["raw", "raw_payment_transactions"]),
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_BRONZE_GROUP,
    kinds=_BRONZE_KINDS,
    description=(
        "Daily payment transactions across the existing servicing book, landed by the Fabric "
        "pipeline migrated from the legacy SSIS payments-extract package."
    ),
)
def raw_payment_transactions(
    context: dg.AssetExecutionContext, fabric: FabricPipelineResource
) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_raw_payment_transactions")
    frame = generators.generate_payment_transactions(event_date)

    conn = connect_with_retry(demo_duckdb_path())
    try:
        upsert_partition(
            conn,
            "raw",
            "payment_transactions",
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

    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "fabric_run_status": run["status"],
            "total_collected": dg.MetadataValue.text(f"${frame['amount'].sum():,.0f}"),
        }
    )


@dg.asset(
    key=dg.AssetKey(["raw", "raw_credit_bureau_pull"]),
    partitions_def=DATE_PARTITIONS_DEF,
    group_name=_BRONZE_GROUP,
    kinds=_BRONZE_KINDS,
    description=(
        "Daily credit-bureau pulls for newly originated borrowers, landed by the Fabric "
        "pipeline migrated from the legacy SSIS bureau-pull package."
    ),
)
def raw_credit_bureau_pull(context: dg.AssetExecutionContext, fabric: FabricPipelineResource) -> dg.MaterializeResult:
    event_date = context.partition_key
    run = fabric.trigger_and_wait(context, pipeline_item_id="pl_raw_credit_bureau_pull")
    frame = generators.generate_credit_bureau_pull(event_date)

    conn = connect_with_retry(demo_duckdb_path())
    try:
        upsert_partition(
            conn,
            "raw",
            "credit_bureau_pull",
            frame,
            match={"pull_date": event_date},
            ddl_columns={
                "pull_id": "VARCHAR",
                "borrower_id": "VARCHAR",
                "bureau": "VARCHAR",
                "credit_score": "INTEGER",
                "pull_date": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(
        metadata={"dagster/row_count": len(frame), "fabric_run_status": run["status"]}
    )


@dg.asset(
    key=dg.AssetKey(["raw", "raw_dealer_floorplan_feed"]),
    partitions_def=FLOORPLAN_MULTI_PARTITIONS_DEF,
    group_name=_BRONZE_GROUP,
    kinds=_BRONZE_KINDS,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=15),
    description=(
        "Daily dealer floorplan-financing advances, one batch per regional dealer group, landed "
        "by the Fabric pipeline migrated from the legacy SSIS floorplan-extract package. "
        "The one genuinely flaky source in this demo -- dealer-side SFTP drops from ~240 "
        "individual dealer relationships time out often enough in production to justify a real "
        "retry policy, unlike the other single-vendor bronze feeds."
    ),
)
def raw_dealer_floorplan_feed(
    context: dg.AssetExecutionContext, fabric: FabricPipelineResource
) -> dg.MaterializeResult:
    keys = context.partition_key.keys_by_dimension
    event_date, dealer_group = keys["date"], keys["dealer_group"]
    corrected = fabric_source_state.is_corrected(event_date, dealer_group)

    run = fabric.trigger_and_wait(context, pipeline_item_id=f"pl_raw_dealer_floorplan_feed_{dealer_group}")
    frame = generators.generate_dealer_floorplan_feed(event_date, dealer_group, corrected=corrected)

    conn = connect_with_retry(demo_duckdb_path())
    try:
        upsert_partition(
            conn,
            "raw",
            "dealer_floorplan_feed",
            frame,
            match={"advance_date": event_date, "dealer_group": dealer_group},
            ddl_columns={
                "floorplan_advance_id": "VARCHAR",
                "dealer_id": "VARCHAR",
                "dealer_group": "VARCHAR",
                "vehicle_vin": "VARCHAR",
                "advance_date": "VARCHAR",
                "advance_amount": "DOUBLE",
                "curtailment_due_date": "VARCHAR",
                "loan_id": "VARCHAR",
            },
        )
    finally:
        conn.close()

    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "fabric_run_status": run["status"],
            "source_corrected": corrected,
        }
    )
