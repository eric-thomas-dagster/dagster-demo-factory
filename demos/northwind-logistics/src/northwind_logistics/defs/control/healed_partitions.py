"""The entire "heal the anomaly" action, driven from the Dagster UI.

Materializing this asset with `healed: ["regional_ltl_b|2026-08-21"]` in the
launchpad config marks that carrier/day partition healed. `carrier_rate_raw`
(see `components/carrier_rate_feed.py`) checks this asset's latest
materialization on every run and skips the planted anomaly for any partition
key in the healed set.

State lives in Dagster's own event log (this asset's materialization
metadata), not a local file -- Dagster+ Serverless gives each run its own
ephemeral disk, so a local file written by one run would not be visible to
the next. See `demo_data/warehouse.py` for the same constraint on the
DuckDB file.

To reset the demo, materialize this asset again with `healed: []`.
"""

import dagster as dg


class HealPartitionsConfig(dg.Config):
    healed: list[str] = []


@dg.asset(
    key_prefix=["demo_control"],
    group_name="demo_control",
    description=(
        "Demo control surface: the set of carrier/day partitions to serve clean "
        "data for even though they match the planted anomaly. Materialize with "
        "healed: [\"regional_ltl_b|2026-08-21\"] to heal the money-shot anomaly; "
        "materialize with healed: [] to reset."
    ),
)
def healed_partitions(context: dg.AssetExecutionContext, config: HealPartitionsConfig) -> dg.MaterializeResult:
    context.log.info("Healed partitions set to: %s", config.healed)
    return dg.MaterializeResult(metadata={"healed": dg.MetadataValue.json(config.healed)})
