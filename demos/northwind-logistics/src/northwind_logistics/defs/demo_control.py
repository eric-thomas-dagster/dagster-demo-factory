"""Jobs that drive the money-shot recovery sequence and reset the demo.

The planted anomaly (missing `regional_ltl_b` data for `2026-08-21`) is
seeded deterministically, so a naive rematerialize would regenerate the same
missing data. `heal_carrier_rate_partition_job` flips the healed-state file
that `demo_data/generators.py` consults, entirely inside the Dagster UI --
no YAML edit, no terminal. `reset_demo_job` clears it so the demo can be run
again from a broken state.
"""

import dagster as dg

from northwind_logistics.demo_data.heal_state import ANOMALY_DATE, mark_healed, reset_healed_state


class HealPartitionConfig(dg.Config):
    rate_date: str = ANOMALY_DATE


@dg.op(name="heal_carrier_rate_partition")
def heal_carrier_rate_partition(config: HealPartitionConfig) -> None:
    mark_healed(config.rate_date)


@dg.job(name="heal_carrier_rate_partition_job")
def heal_carrier_rate_partition_job() -> None:
    heal_carrier_rate_partition()


@dg.op(name="reset_demo_state")
def reset_demo_state() -> None:
    reset_healed_state()


@dg.job(name="reset_demo_job")
def reset_demo_job() -> None:
    reset_demo_state()


@dg.definitions
def demo_control_defs() -> dg.Definitions:
    return dg.Definitions(jobs=[heal_carrier_rate_partition_job, reset_demo_job])
