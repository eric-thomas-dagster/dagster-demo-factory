"""Mock Fabric run-history entries for the polling sensor's observation seam.

Represents runs of migrated Fabric pipelines that Dagster did not trigger --
SFS's own homegrown scheduler still driving a package mid-migration, or an
operator running one by hand. `DemoFabricWorkspaceComponent`'s polling sensor
(`polling_sensor: true` in defs.yaml) surfaces these as `AssetObservation`
events so they land in the same lineage graph, with the same freshness
tracking, as anything Dagster itself triggered -- the literal coexistence
story the brief asks the money shot to prove.

Deterministic and static -- there is no live Fabric run-history API to poll
in demo mode, so the sensor rotates through this fixed list instead.
"""

EXTERNALLY_TRIGGERED_ITEMS = [
    {
        "item_name": "raw_credit_bureau_pull",
        "asset_key": ["raw_credit_bureau_pull"],
        "triggered_by": "SFS homegrown scheduler -- package not yet cut over to Dagster",
    },
    {
        "item_name": "raw_dealer_floorplan_feed",
        "asset_key": ["raw_dealer_floorplan_feed"],
        "dealer_group": "south",
        "triggered_by": "Operator ran the Fabric pipeline by hand mid-migration",
    },
]
