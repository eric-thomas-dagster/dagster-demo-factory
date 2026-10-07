"""The one partition scheme every tenant-scoped asset shares.

`date` x `tenant`: a daily time dimension crossed with a static tenant
dimension. 20 demo tenants stand in for Skygen's ~170 per-client databases --
growing to 170 is a longer `TENANTS` list (or one more entry in the
`tenants:` YAML block), never a new asset or component instance.
"""

import dagster as dg

DEMO_TENANT_COUNT = 20
TENANTS = [f"tenant_{n:03d}" for n in range(1, DEMO_TENANT_COUNT + 1)]
START_DATE = "2026-09-28"

TENANT_DAILY_PARTITIONS = dg.MultiPartitionsDefinition(
    {
        "date": dg.DailyPartitionsDefinition(start_date=START_DATE, end_offset=0),
        "tenant": dg.StaticPartitionsDefinition(TENANTS),
    }
)


def split_partition_key(partition_key: str) -> tuple[str, str]:
    """Return `(tenant, date)` for a multi-partition key."""
    keys = partition_key.keys_by_dimension if isinstance(partition_key, dg.MultiPartitionKey) else None
    if keys is None:
        raise ValueError(f"expected a MultiPartitionKey, got {partition_key!r}")
    return keys["tenant"], keys["date"]
