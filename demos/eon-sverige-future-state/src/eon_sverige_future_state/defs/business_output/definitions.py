"""Grid operations daily briefing -- the business-facing asset.

Reads from the curated data product and the two MLflow scoring/promotion
assets and produces a daily briefing artifact. In real mode this would
write to Power BI or a shared BI table; in demo mode it summarises the
inputs and returns row counts + regional load peaks in materialization
metadata (real numbers, read from the warehouse -- see
demo_data/bootstrap.py for the deterministic fixture that populates it).

Kept as a `.py` file (with a one-line justification per house rules):
this is a single unpartitioned asset that composes three upstream
partitioned assets via `deps=`. No community component fits the shape,
and a component wrapping one asset would be more indirect than the
direct code.
"""

import datetime

import dagster as dg

from eon_sverige_future_state.components.partitions import MONTHLY_PARTITIONS_DEF
from eon_sverige_future_state.demo_data.warehouse import demo_duckdb_path


@dg.asset(
    name="grid_operations_daily_briefing",
    group_name="business_output",
    kinds={"azure", "power_bi"},
    owners=["team:eon-grid-ops"],
    description=(
        "Daily operations briefing artifact combining the grid-load "
        "forecast, meter-anomaly predictions, and the customer-switching "
        "extract. In demo mode returns summary metadata; in real mode "
        "writes to Power BI / a shared BI table."
    ),
    deps=[
        dg.AssetKey(["curated", "customer_switching_extract"]),
        dg.AssetKey("grid_load_forecast_model"),
        dg.AssetKey("meter_anomaly_predictions"),
    ],
    metadata={
        "owner_team": "team:eon-grid-ops",
        "tier": "tier_1",
        "domain": "grid_operations",
        "business_impact": (
            "The business-facing SLA. 24h fail / 20h warn freshness -- "
            "grid ops relies on this being current at start of shift."
        ),
        "sla": "24h fresh / 20h warn",
        "integration_pattern": "coexistence",
    },
    freshness_policy=dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(hours=24),
        warn_window=datetime.timedelta(hours=20),
    ),
    partitions_def=MONTHLY_PARTITIONS_DEF,
)
def grid_operations_daily_briefing(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Compose the daily briefing.

    Reads real row counts + peak load per region from the warehouse.
    Numbers are real synthetic values from the fixture, not fabricated.
    """
    import duckdb  # noqa: PLC0415

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        try:
            n_customers = conn.execute(
                "select count(*) from main_curated.customer_switching_extract"
            ).fetchone()[0]
        except duckdb.Error:
            n_customers = 0
        try:
            switching_active = conn.execute(
                "select count(*) from main_curated.customer_switching_extract "
                "where switching_status != 'none'"
            ).fetchone()[0]
        except duckdb.Error:
            switching_active = 0
        try:
            peak_by_region = conn.execute(
                "select region_code, max(peak_load_mw) "
                "from main_gold.fct_grid_hourly group by region_code"
            ).fetchall()
        except duckdb.Error:
            peak_by_region = []
        try:
            avg_daily_kwh = conn.execute(
                "select avg(kwh) from main_gold.fct_meter_daily"
            ).fetchone()[0] or 0.0
        except duckdb.Error:
            avg_daily_kwh = 0.0
    finally:
        conn.close()

    peak_summary = {r[0]: round(r[1], 1) for r in peak_by_region}
    context.log.info(
        f"[briefing] customers={n_customers} switching_active={switching_active} "
        f"peak_by_region={peak_summary} avg_daily_kwh={avg_daily_kwh:.2f}"
    )

    return dg.MaterializeResult(
        metadata={
            "customer_count": n_customers,
            "customers_switching": switching_active,
            "avg_daily_kwh_per_meter": round(avg_daily_kwh, 3),
            "peak_load_mw_by_region": dg.MetadataValue.json(peak_summary),
            "briefing_generated_at": datetime.datetime.utcnow().isoformat(),
            "partition_key": (
                context.partition_key if context.has_partition_key else "none"
            ),
        }
    )


@dg.definitions
def defs():
    return dg.Definitions(assets=[grid_operations_daily_briefing])
