"""Custom component: bronze lending-data ingestion.

Registry gap (rung 4 of the escalation ladder): searched "fivetran",
"eventbridge", "sqs", "s3 sensor", "schema change", "s3" (category=resource)
and "s3" (category=sensor) in the community registry. `sqs_monitor` and
`s3_monitor` exist but poll a real queue/bucket with no demo-mode affordance
-- using either here would mean either standing up real AWS infrastructure
(explicitly out of scope per the brief) or leaving the demo unable to run
with zero setup. `fivetran_assets` imports each Fivetran connector as an
external asset keyed by the connector's own tables -- it has no notion of a
`(date, product_line)` partition, so it can't produce the targeted-recovery
story the brief's `MultiPartitionsDefinition` directive calls for. Same gap
LEARNINGS.md already records for ADLS; this run confirms it also holds for
S3/EventBridge/SQS. One component class, instantiated once per feed via
`defs.yaml`, keeps the three bronze assets from being three near-identical
hand-rolled Python files.

Follows `templates/demo_mode_pattern.py`: the network seam is `_fetch`, the
single method that would poll the real vendor system (Fivetran's API, or an
S3 object read). Everything else -- asset spec, partition definition,
metadata -- is identical whether `demo_mode` is true or false.
"""

import dagster as dg
import pandas as pd
from pydantic import Field

from kapitus.components.partitions import DATE_PRODUCT_PARTITIONS_DEF
from kapitus.components.resources import DemoS3Resource
from kapitus.demo_data import generators
from kapitus.demo_data.warehouse import connect_with_retry, demo_duckdb_path, upsert_partition

_FEED_CONFIG = {
    "loan_applications": {
        "asset_key": ["raw", "loan_applications"],
        "table": "loan_applications",
        "date_column": "application_date",
        "description": (
            "Loan applications and their funding decisions, pulled from the loan "
            "origination system via Fivetran."
        ),
        "kinds": {"fivetran", "aws"},
        "ddl_columns": {
            "application_id": "VARCHAR", "application_date": "VARCHAR", "product_line": "VARCHAR",
            "business_id": "VARCHAR", "business_state": "VARCHAR", "requested_amount": "DOUBLE",
            "funding_status": "VARCHAR", "funded_amount": "DOUBLE", "apr": "DOUBLE", "term_months": "BIGINT",
        },
    },
    "bank_statement_data": {
        "asset_key": ["raw", "bank_statement_data"],
        "table": "bank_statement_data",
        "date_column": "statement_date",
        "description": (
            "OCR-derived bank statement analysis for each day's applicants, landed from S3."
        ),
        "kinds": {"aws"},
        "ddl_columns": {
            "statement_id": "VARCHAR", "statement_date": "VARCHAR", "product_line": "VARCHAR",
            "business_id": "VARCHAR", "avg_daily_balance": "DOUBLE", "nsf_count_90d": "BIGINT",
            "monthly_revenue_estimate": "DOUBLE", "cash_flow_score": "BIGINT",
        },
    },
    "credit_bureau_pulls": {
        "asset_key": ["raw", "credit_bureau_pulls"],
        "table": "credit_bureau_pulls",
        "date_column": "pull_date",
        "description": (
            "Commercial credit bureau pulls for each day's applicants, landed from S3 via Lambda."
        ),
        "kinds": {"aws"},
        "ddl_columns": {
            "pull_id": "VARCHAR", "pull_date": "VARCHAR", "product_line": "VARCHAR", "business_id": "VARCHAR",
            "bureau_name": "VARCHAR", "business_credit_score": "BIGINT", "personal_credit_score": "BIGINT",
            "years_in_business": "DOUBLE", "existing_debt_obligations": "DOUBLE",
        },
    },
}

_GENERATORS = {
    "loan_applications": generators.generate_loan_applications_frame,
    "bank_statement_data": generators.generate_bank_statement_frame,
    "credit_bureau_pulls": generators.generate_credit_bureau_frame,
}


class BronzeFeedComponent(dg.Component, dg.Resolvable, dg.Model):
    """Ingests one day's, one product line's batch from one Kapitus bronze feed.

    Real mode polls the real source (Fivetran's REST API for loan
    applications, an S3 object read for the other two). Demo mode generates
    a deterministic synthetic batch. Asset key, spec, partitions, and
    metadata are identical in both modes -- only `_fetch`, the network
    boundary, differs.
    """

    feed: str = Field(description=f"Which bronze feed: one of {sorted(_FEED_CONFIG)}.")
    landing: DemoS3Resource = Field(default_factory=DemoS3Resource)
    demo_mode: bool = Field(default=True, description="Serve a synthetic batch instead of polling the real source.")
    demo_seed: int = Field(default=20260825, description="Seed for deterministic synthetic generation.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        config = _FEED_CONFIG[self.feed]
        spec = dg.AssetSpec(
            key=dg.AssetKey(config["asset_key"]),
            description=config["description"],
            group_name="ingestion",
            kinds=config["kinds"],
            partitions_def=DATE_PRODUCT_PARTITIONS_DEF,
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(specs=[spec], name=f"raw_{self.feed}")
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            partition_key = context.partition_key
            event_date = partition_key.keys_by_dimension["date"]
            product_line = partition_key.keys_by_dimension["product_line"]
            frame = self._fetch(context, event_date, product_line)

            self.landing.write_landing_object(
                bucket="kapitus-bronze-landing",
                key=f"{self.feed}/{product_line}/{event_date}.json",
                data=frame.to_json(orient="records").encode("utf-8"),
            )

            date_column = config["date_column"]
            conn = connect_with_retry(demo_duckdb_path())
            try:
                upsert_partition(
                    conn,
                    schema="raw",
                    table=config["table"],
                    df=frame,
                    match={date_column: event_date, "product_line": product_line},
                    ddl_columns=config["ddl_columns"],
                )
            finally:
                conn.close()

            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": self.demo_mode,
                    date_column: event_date,
                    "product_line": product_line,
                }
            )

        return dg.Definitions(assets=[_asset])

    def _fetch(self, context: dg.AssetExecutionContext, event_date: str, product_line: str) -> pd.DataFrame:
        """The network seam. Real mode polls the source; demo mode fakes it."""
        if not self.demo_mode:
            raise NotImplementedError(
                f"Real-mode fetch for the {self.feed} feed is not implemented in this demo. "
                "Replace this branch with the real Fivetran/S3 client when connecting to "
                "Kapitus's actual systems."
            )
        return _GENERATORS[self.feed](event_date, product_line, self.demo_seed)
