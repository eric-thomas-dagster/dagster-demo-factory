"""Demo-mode control state.

Northwind's money-shot recovery flow needs a way to "heal" the planted
anomaly entirely from the Dagster UI -- no YAML edit, no terminal -- and
that healing has to survive across separate runs.

Dagster+ Serverless gives each run its own ephemeral disk (confirmed via
Dagster's own docs: runs get "ephemeral disk", nothing shared between
them), so a local state file written by one run is not visible to the next
run. Dagster's event log does not have that problem -- it is Dagster+'s own
managed, cross-run storage -- so the healed-partitions ledger lives there
instead: as metadata on the latest materialization of the `healed_partitions`
control asset. Re-materializing that asset (via the UI's launchpad) is the
entire "heal" action.
"""

from __future__ import annotations

import dagster as dg

HEALED_PARTITIONS_ASSET_KEY = dg.AssetKey(["demo_control", "healed_partitions"])

EXPECTED_CARRIERS = ["fedex", "ups", "regional_ltl_a", "regional_ltl_b"]

ANOMALY_CARRIER = "regional_ltl_b"
ANOMALY_DATE = "2026-08-21"
# MultiPartitionKey stringifies dimensions in alphabetical-by-name order
# ("carrier" < "date"), not declaration order -- verified against
# dagster 1.13.19: MultiPartitionKey({"date": ..., "carrier": ...}) still
# prints "<carrier>|<date>".
ANOMALY_PARTITION_KEY = f"{ANOMALY_CARRIER}|{ANOMALY_DATE}"

DEMO_WINDOW_START = "2026-08-15"
DEMO_WINDOW_END = "2026-08-24"


def get_healed_partitions(instance: dg.DagsterInstance) -> frozenset[str]:
    """Reads the current healed-partition set from the event log.

    Returns an empty set until `healed_partitions` has ever been
    materialized -- the natural "not healed yet" starting state.
    """
    event = instance.get_latest_materialization_event(HEALED_PARTITIONS_ASSET_KEY)
    if event is None or event.asset_materialization is None:
        return frozenset()
    healed = event.asset_materialization.metadata.get("healed")
    if healed is None:
        return frozenset()
    return frozenset(healed.value)


def is_healed(instance: dg.DagsterInstance, partition_key: str) -> bool:
    return partition_key in get_healed_partitions(instance)
