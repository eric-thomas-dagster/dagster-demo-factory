"""Warning: the tenant's data was ready ahead of its SLA deadline.

The "customers spot SLA misses first" pain, inverted: this surfaces a tight
or missed SLA to the data team before the client sees it. Warning severity --
a late tenant should be visible, not stop other tenants. Per `(tenant, date)`.
"""

import dagster as dg

from skygen.demo_data.warehouse import connect
from skygen.partitions import split_partition_key

MINIMUM_SLACK_MINUTES = 60


@dg.asset_check(
    asset=dg.AssetKey(["mart", "mart_client_sla_scorecard"]),
    description=f"Warns when a tenant's data was ready less than {MINIMUM_SLACK_MINUTES} minutes before its SLA deadline.",
)
def client_sla_data_ready(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    tenant, day = split_partition_key(context.partition_key)
    with connect(read_only=True) as conn:
        row = conn.execute(
            "select sla_slack_minutes, sla_met from main_mart.mart_client_sla_scorecard "
            "where tenant_id = ? and report_date = ?",
            [tenant, day],
        ).fetchone()
    if row is None:
        return dg.AssetCheckResult(passed=False, severity=dg.AssetCheckSeverity.WARN, description="No scorecard row.")
    slack, met = row
    return dg.AssetCheckResult(
        passed=bool(met) and slack >= MINIMUM_SLACK_MINUTES,
        severity=dg.AssetCheckSeverity.WARN,
        description=f"{tenant} data ready {slack} minutes before its SLA deadline on {day}.",
        metadata={"tenant": tenant, "date": day, "sla_slack_minutes": slack},
    )
