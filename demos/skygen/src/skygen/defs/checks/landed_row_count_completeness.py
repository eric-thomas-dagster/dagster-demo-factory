"""Blocking: Fivetran landed every row the client's source database holds.

The brief's named pain -- a sync that lands "98% of the rows" and looks
successful. One check definition covers all three landed tables and runs per
`(tenant, date)` partition. It is blocking, so dbt refuses to build on an
incomplete landing instead of computing something quietly wrong. In
production a failure here fires Dagster+ alerting; nothing to build.

The denominator is what the source system itself reports
(`expected_row_count`); the numerator is what is in the warehouse.
"""

import dagster as dg

from skygen.demo_data.landing import LANDING_SCHEMA
from skygen.demo_data.tenant_source import TABLES, expected_row_count
from skygen.demo_data.warehouse import connect
from skygen.partitions import split_partition_key

MINIMUM_LANDED_FRACTION = 0.999

_KEYS = [dg.AssetKey([LANDING_SCHEMA, table]) for table in TABLES]


@dg.multi_asset_check(
    specs=[
        dg.AssetCheckSpec(
            name="landed_row_count_completeness",
            asset=key,
            blocking=True,
            description=(
                f"Fails when the warehouse holds under {MINIMUM_LANDED_FRACTION:.1%} of the "
                "rows the client's source database reports for the tenant and day."
            ),
        )
        for key in _KEYS
    ],
    can_subset=True,
)
def landed_row_count_completeness(context: dg.AssetCheckExecutionContext):
    tenant, day = split_partition_key(context.partition_key)
    with connect(read_only=True) as conn:
        for key in _KEYS:
            if dg.AssetCheckKey(key, "landed_row_count_completeness") not in context.selected_asset_check_keys:
                continue
            table = key.path[-1]
            landed = conn.execute(
                f"select count(*) from {LANDING_SCHEMA}.{table} where tenant_id = ? and report_date = ?",
                [tenant, day],
            ).fetchone()[0]
            expected = expected_row_count(table, tenant, day)
            fraction = landed / expected if expected else 0.0
            yield dg.AssetCheckResult(
                asset_key=key,
                check_name="landed_row_count_completeness",
                passed=fraction >= MINIMUM_LANDED_FRACTION,
                severity=dg.AssetCheckSeverity.ERROR,
                description=f"{landed}/{expected} rows landed for {tenant} on {day} ({fraction:.1%}).",
                metadata={"tenant": tenant, "date": day, "rows_landed": landed, "rows_in_source": expected},
            )
