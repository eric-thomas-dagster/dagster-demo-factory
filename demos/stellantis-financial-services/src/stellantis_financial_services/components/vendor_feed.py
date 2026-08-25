"""Custom component: bronze vendor-file ingestion.

Registry gap (rung 4 of the escalation ladder): no registry component
supports a partitioned, demo-mode-fakeable "poll a vendor file drop" pattern
-- searched "microsoft fabric", "onelake", "azure data lake", "sql server",
"power bi", "teams webhook", "notification", "email" (dataframe_to_adls /
fabric_lakehouse_* are sinks that write OUT to the lake, not sources that
read a vendor's inbound file; adls_monitor only detects new blobs, it
doesn't parse a vendor schema). Same gap LEARNINGS.md already records for
`database_replication` / `rest_api_fetcher` / `odata_ingestion`. One
component class, instantiated once per vendor feed via `defs.yaml`, keeps
the five bronze assets from being five near-identical hand-rolled Python
files.

Follows `templates/demo_mode_pattern.py`: the network seam is `_fetch`, the
single method that would poll the real vendor system. Everything else --
asset spec, partition definition, metadata, retry policy -- is identical
whether `demo_mode` is true or false.
"""

import dagster as dg
import pandas as pd
from pydantic import Field

from stellantis_financial_services.components.partitions import DAILY_PARTITIONS_DEF
from stellantis_financial_services.components.resources import DemoADLS2Resource
from stellantis_financial_services.demo_data import generators
from stellantis_financial_services.demo_data.vendor_state import is_corrected
from stellantis_financial_services.demo_data.warehouse import connect_with_retry, demo_duckdb_path, upsert_partition

_FEED_CONFIG = {
    "loan_originations": {
        "asset_key": ["raw", "loan_originations"],
        "table": "loan_originations",
        "match_column": "contract_date",
        "description": "Dealer-submitted auto loan origination contracts, one batch per vendor file drop.",
        "ddl_columns": {
            "loan_id": "VARCHAR", "contract_date": "VARCHAR", "dealer_id": "VARCHAR",
            "borrower_id": "VARCHAR", "vehicle_vin": "VARCHAR", "amount_financed": "DOUBLE",
            "apr": "DOUBLE", "term_months": "BIGINT", "product_type": "VARCHAR", "borrower_state": "VARCHAR",
        },
    },
    "lease_originations": {
        "asset_key": ["raw", "lease_originations"],
        "table": "lease_originations",
        "match_column": "contract_date",
        "description": "Dealer-submitted lease origination contracts, one batch per vendor file drop.",
        "ddl_columns": {
            "lease_id": "VARCHAR", "contract_date": "VARCHAR", "dealer_id": "VARCHAR",
            "borrower_id": "VARCHAR", "vehicle_vin": "VARCHAR", "capitalized_cost": "DOUBLE",
            "residual_value": "DOUBLE", "monthly_payment": "DOUBLE", "term_months": "BIGINT",
            "product_type": "VARCHAR", "borrower_state": "VARCHAR",
        },
    },
    "payment_transactions": {
        "asset_key": ["raw", "payment_transactions"],
        "table": "payment_transactions",
        "match_column": "payment_date",
        "description": "Servicer payment/collections feed for loan and lease contracts.",
        "ddl_columns": {
            "payment_id": "VARCHAR", "contract_id": "VARCHAR", "contract_type": "VARCHAR",
            "payment_date": "VARCHAR", "amount_paid": "DOUBLE", "days_past_due": "BIGINT",
            "payment_method": "VARCHAR",
        },
    },
    "dealer_floorplan": {
        "asset_key": ["raw", "dealer_floorplan_feed"],
        "table": "dealer_floorplan_feed",
        "match_column": "advance_date",
        "description": (
            "Dealer floorplan (inventory financing) advance feed. Dealer-submitted, not "
            "system-to-system -- the one source in this demo with a genuine RetryPolicy."
        ),
        "ddl_columns": {
            "floorplan_advance_id": "VARCHAR", "dealer_id": "VARCHAR", "advance_date": "VARCHAR",
            "vehicle_vin": "VARCHAR", "advance_amount": "DOUBLE", "curtailment_due_date": "VARCHAR",
        },
    },
    "credit_bureau_pull": {
        "asset_key": ["raw", "credit_bureau_pull"],
        "table": "credit_bureau_pull",
        "match_column": "score_date",
        "description": "Credit bureau pull for each day's new loan and lease borrowers.",
        "ddl_columns": {
            "borrower_id": "VARCHAR", "bureau_name": "VARCHAR", "bureau_score": "BIGINT",
            "score_date": "VARCHAR", "inquiry_count_6mo": "BIGINT",
        },
    },
}


class VendorFeedComponent(dg.Component, dg.Resolvable, dg.Model):
    """Ingests one day's batch from one SFS vendor feed into the bronze schema.

    Real mode polls the vendor's actual file drop (SFTP/API, per vendor).
    Demo mode generates a deterministic synthetic batch. Asset key, spec,
    partitions, and metadata are identical in both modes -- only `_fetch`,
    the network boundary, differs.
    """

    feed: str = Field(description=f"Which vendor feed: one of {sorted(_FEED_CONFIG)}.")
    landing: DemoADLS2Resource = Field(default_factory=DemoADLS2Resource)
    demo_mode: bool = Field(default=True, description="Serve a synthetic batch instead of polling the real vendor feed.")
    demo_seed: int = Field(default=20260825, description="Seed for deterministic synthetic generation.")
    retryable: bool = Field(
        default=False,
        description="Attach a RetryPolicy -- reserve this for genuinely flaky, dealer-submitted sources.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        config = _FEED_CONFIG[self.feed]
        spec = dg.AssetSpec(
            key=dg.AssetKey(config["asset_key"]),
            description=config["description"],
            group_name="bronze",
            kinds={"azure"},
            partitions_def=DAILY_PARTITIONS_DEF,
            metadata={"demo_mode": self.demo_mode},
        )

        @dg.multi_asset(
            specs=[spec],
            name=f"raw_{self.feed}",
            retry_policy=dg.RetryPolicy(max_retries=3, delay=10, backoff=dg.Backoff.EXPONENTIAL) if self.retryable else None,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            event_date = context.partition_key
            frame = self._fetch(context, event_date)

            self.landing.write_landing_blob(
                container="bronze-landing",
                path=f"{self.feed}/{event_date}.json",
                data=frame.to_json(orient="records").encode("utf-8"),
            )

            conn = connect_with_retry(demo_duckdb_path())
            try:
                upsert_partition(
                    conn,
                    schema="raw",
                    table=config["table"],
                    df=frame,
                    match={config["match_column"]: event_date},
                    ddl_columns=config["ddl_columns"],
                )
            finally:
                conn.close()

            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(frame),
                    "demo_mode": self.demo_mode,
                    config["match_column"]: event_date,
                }
            )

        return dg.Definitions(assets=[_asset])

    def _fetch(self, context: dg.AssetExecutionContext, event_date: str) -> pd.DataFrame:
        """The network seam. Real mode polls the vendor; demo mode fakes it."""
        if not self.demo_mode:
            raise NotImplementedError(
                f"Real-mode polling for the {self.feed} vendor feed is not implemented in this "
                "demo. Replace this branch with the real vendor client (SFTP/API per vendor) "
                "when connecting to SFS's actual feed."
            )

        if self.feed == "dealer_floorplan":
            corrected = is_corrected("dealer_floorplan", event_date)
            frame = generators.generate_dealer_floorplan_frame(event_date, self.demo_seed, corrected=corrected)
            if not corrected:
                context.log.info(
                    "Simulated dealer floorplan feed for %s has a malformed record (missing VIN). "
                    "Run `python -m stellantis_financial_services.demo_data.simulate_corrected_feed` "
                    "then rematerialize this partition to pick up the corrected file.",
                    event_date,
                )
            return frame

        generator = {
            "loan_originations": generators.generate_loan_originations_frame,
            "lease_originations": generators.generate_lease_originations_frame,
            "payment_transactions": generators.generate_payment_transactions_frame,
            "credit_bureau_pull": generators.generate_credit_bureau_frame,
        }[self.feed]
        return generator(event_date, self.demo_seed)
