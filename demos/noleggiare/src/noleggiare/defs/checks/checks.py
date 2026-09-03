"""The three asset checks for the Noleggiare demo, each mapped to a
directive in the brief's Asset checks section. Custom Python because
assertion logic is business logic, not something a component wraps --
same registry gap identified in the detroit-dwsd build
(`component-feedback/2026-08-28-graph-first-assets.md` covers the sibling
graph-first-assets gap; no declarative completeness/reconciliation check
component exists either, not re-searched here since the gap is unchanged).
Kept in one file rather than three: each check is a handful of lines and
none share code.

Per the brief's graph-first Fidelity directive, there's no real contract,
VIN, or row count to compute here -- every check always passes and reports
the rule it would enforce against real data in production. The talk track
is what each check catches when it's wired to live Postgres feeds, not a
live number today. (This is unaffected by the Axis-1 IO manager fix
elsewhere in this build: the upstream assets now write real, if trivial,
stub rows through a real IO manager, but the check *logic* stays the
brief-specified static pass -- Axis 2 fidelity, unchanged.)
"""

import dagster as dg

EXPECTED_DAILY_ROW_COUNT_MIN = 200
EXPECTED_DAILY_ROW_COUNT_MAX = 2_000


@dg.asset_check(
    asset=dg.AssetKey(["fact_rental_contract"]),
    blocking=True,
    description=(
        "Fails when any contract row is missing customer_id, vehicle_id, or "
        "company_id -- would block every downstream revenue figure from "
        "computing on an unresolvable contract."
    ),
)
def fact_rental_contract_completeness(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps to the brief's first Asset checks directive -- Raffaele's own
    stated pain: "wants to gradually introduce data quality checks... not
    just ETL execution."
    """
    return dg.AssetCheckResult(
        passed=True,
        description=(
            "Every contract row resolves customer_id, vehicle_id, and "
            "company_id (graph-first demo -- no contract data computed). In "
            "production, would fail on any row missing one of the three."
        ),
        metadata={
            "check_semantics": (
                "customer_id IS NOT NULL AND vehicle_id IS NOT NULL AND "
                "company_id IS NOT NULL for every row"
            ),
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["dim_vehicle"]),
    blocking=True,
    description=(
        "Fails when a VIN appears as active fleet inventory in "
        "noleggiare_rental_ops and active dealer inventory in "
        "tomasi_dealer_ops at the same time -- would block the cross-company "
        "vehicle dimension from carrying a double-counted vehicle into "
        "either company's finance figures."
    ),
)
def dim_vehicle_cross_company_consistency(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps to the brief's second Asset checks directive -- the single check
    that most directly demonstrates why a shared, governed platform beats
    each company running its own scripts: "two companies need to coordinate
    around a shared BI/data platform."
    """
    return dg.AssetCheckResult(
        passed=True,
        description=(
            "No VIN is simultaneously active fleet inventory and active "
            "dealer inventory (graph-first demo -- no vehicle data computed). "
            "In production, would fail on any VIN present as an active row "
            "in both source systems on the same day."
        ),
        metadata={
            "check_semantics": (
                "NOT (vin IN active_fleet_vehicles AND vin IN active_dealer_inventory) "
                "for every VIN, evaluated daily"
            ),
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["fact_finance_consolidated_daily"]),
    blocking=False,
    description=(
        f"Warns when a partition's row count falls outside the "
        f"[{EXPECTED_DAILY_ROW_COUNT_MIN}, {EXPECTED_DAILY_ROW_COUNT_MAX}] "
        "band expected at steady state -- surfaced, not blocking."
    ),
)
def fact_finance_consolidated_daily_volume_band(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps to the brief's third Asset checks directive: catches a source
    going quiet before it silently starves the Qlik Cloud dashboard both
    companies' Finance teams read every morning. Warning, not blocking -- a
    volume dip is worth surfacing, not worth halting the day's consolidated
    figure over.
    """
    partition_key = context.partition_key
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"{partition_key}: row count within the expected "
            f"[{EXPECTED_DAILY_ROW_COUNT_MIN}, {EXPECTED_DAILY_ROW_COUNT_MAX}] "
            "band (graph-first demo -- no rows computed). In production, "
            "would warn outside that band."
        ),
        metadata={
            "expected_row_count_min": EXPECTED_DAILY_ROW_COUNT_MIN,
            "expected_row_count_max": EXPECTED_DAILY_ROW_COUNT_MAX,
            "check_semantics": (
                f"{EXPECTED_DAILY_ROW_COUNT_MIN} <= row_count <= "
                f"{EXPECTED_DAILY_ROW_COUNT_MAX} per (date, company) partition"
            ),
            "partition": str(partition_key),
        },
    )
