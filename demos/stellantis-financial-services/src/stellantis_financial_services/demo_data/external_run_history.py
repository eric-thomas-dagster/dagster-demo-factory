"""Mock Fabric run-history entries for the polling sensor's observation seam.

Represents runs of **already-migrated** Fabric pipelines that Dagster did not
trigger -- an operator running one by hand, or SFS's own scheduler calling
the Fabric API directly for a package mid-cutover, before Dagster owns its
schedule. `DemoFabricWorkspaceComponent`'s polling sensor (`generate_sensor:
true` in defs.yaml) surfaces these as `AssetObservation` events so they land
in the same lineage graph, with the same freshness tracking, as anything
Dagster itself triggered.

This is a separate, smaller coexistence story from the two genuinely-legacy
assets (`raw_dealer_floorplan_feed`, `raw_credit_bureau_pull`) -- those have
no Fabric pipeline behind them at all and are observed by their own dedicated
sensor instead (`defs/legacy_assets/legacy_assets.py`). Every item here must
be one of the entries in `assets_by_item_name` (defs/fabric_pipelines/defs.yaml)
-- it represents a Fabric pipeline someone ran outside Dagster, not a package
still on SSIS.

Deterministic and static -- there is no live Fabric run-history API to poll
in demo mode, so the sensor rotates through this fixed list instead.
"""

EXTERNALLY_TRIGGERED_ITEMS = [
    {
        "item_name": "raw_payment_transactions",
        "asset_key": ["raw_payment_transactions"],
        "triggered_by": "Operator ran the Fabric pipeline by hand to backfill a late vendor file",
    },
    {
        "item_name": "stg_loan_originations",
        "asset_key": ["stg_loan_originations"],
        "triggered_by": "SFS homegrown scheduler called the Fabric API directly mid-cutover, before Dagster owned this package's schedule",
    },
]
