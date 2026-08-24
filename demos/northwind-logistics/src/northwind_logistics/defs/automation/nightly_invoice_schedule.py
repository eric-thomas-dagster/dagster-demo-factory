"""The real deadline the brief names: nightly invoice/billing batch must
complete before 6am ET for finance. Runs at 5am ET, an hour of slack ahead
of it.
"""

import dagster as dg

invoice_billing_nightly_job = dg.define_asset_job(
    name="invoice_billing_nightly_job",
    selection=dg.AssetSelection.assets(dg.AssetKey(["marts", "invoice_billing_nightly"])),
)

invoice_billing_nightly_schedule = dg.build_schedule_from_partitioned_job(
    invoice_billing_nightly_job,
    hour_of_day=5,
    minute_of_hour=0,
    name="invoice_billing_nightly_schedule",
    description="Runs the invoice/billing chain before the 6am ET finance deadline.",
)
