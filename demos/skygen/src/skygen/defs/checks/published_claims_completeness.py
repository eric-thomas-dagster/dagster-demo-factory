"""Blocking: every claim that reached the transform layer was published.

Gates the Sigma-facing marts. If the published layer lost claims in the join
to member coverage (a bad key, a dropped row), the SLA scorecard would show a
confident, wrong number; this stops the mart from computing instead.
Per `(tenant, date)` partition.
"""

import dagster as dg

from skygen.demo_data.warehouse import connect
from skygen.partitions import split_partition_key


@dg.asset_check(
    asset=dg.AssetKey(["published", "pub_member_claims"]),
    blocking=True,
    description="Fails when the published claims for the tenant and day are fewer than the unified claims they derive from.",
)
def published_claims_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    tenant, day = split_partition_key(context.partition_key)
    with connect(read_only=True) as conn:
        unified = conn.execute(
            "select count(*) from main_transform.tfm_claims_unified where tenant_id = ? and report_date = ?",
            [tenant, day],
        ).fetchone()[0]
        published = conn.execute(
            "select count(*) from main_published.pub_member_claims where tenant_id = ? and report_date = ?",
            [tenant, day],
        ).fetchone()[0]
    return dg.AssetCheckResult(
        passed=unified > 0 and published >= unified,
        description=f"{published} published vs {unified} unified claims for {tenant} on {day}.",
        metadata={"tenant": tenant, "date": day, "published_rows": published, "unified_rows": unified},
    )
