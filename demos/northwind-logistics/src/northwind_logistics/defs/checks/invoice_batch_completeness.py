"""We find out something broke when a customer emails about a missing invoice.

This check is the thing that finds out first instead: it verifies the
nightly invoice batch actually has rows and a positive total for its
partition, before finance opens the numbers at 6am ET.
"""

import dagster as dg

from northwind_logistics.demo_data.warehouse import connect_with_retry, demo_duckdb_path


@dg.asset_check(
    asset=dg.AssetKey(["marts", "invoice_billing_nightly"]),
    description="Verifies the nightly invoice batch has rows and a positive total for its day.",
)
def invoice_batch_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    event_date = context.partition_key

    conn = connect_with_retry(demo_duckdb_path())
    try:
        row = conn.execute(
            "select line_item_count, total_invoiced from main_marts.invoice_billing_nightly where event_date = ?",
            [event_date],
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return dg.AssetCheckResult(
            passed=False,
            description=f"No invoice batch row exists for {event_date}.",
            metadata={"event_date": event_date},
        )

    line_item_count, total_invoiced = row
    if line_item_count <= 0 or total_invoiced is None or total_invoiced <= 0:
        return dg.AssetCheckResult(
            passed=False,
            description=(
                f"Invoice batch for {event_date} has {line_item_count} line items totaling "
                f"{total_invoiced} -- this is what a customer emailing about a missing invoice looks like."
            ),
            metadata={"event_date": event_date, "line_item_count": line_item_count},
        )
    return dg.AssetCheckResult(
        passed=True,
        description=f"{line_item_count} line items totaling {total_invoiced} for {event_date}.",
        metadata={"event_date": event_date, "line_item_count": line_item_count, "total_invoiced": total_invoiced},
    )
