"""The three asset checks for the DWSD demo, each mapped to a pain named in
the brief. Custom Python because assertion logic is business logic, not
something a component wraps -- registry searched for "asset check
declarative yaml" and "row count completeness check" (2026-08-28,
dagster-community-components-cli 0.8.15), neither hit anything relevant.
Kept in one file rather than three: each check is a handful of lines and
none share code, so three files would be pure file-count overhead in a
`defs/` tree that's meant to be predominantly YAML.

Graph-first fidelity means there's no real row count or lab reading to
compute here -- every check always passes and reports the threshold or
required-field set it would enforce against real data in production. The
talk track is what each check catches when it's wired to a live feed, not a
live number today.
"""

import dagster as dg

EXPECTED_METER_READING_ROW_COUNT_FLOOR = 50_000
REQUIRED_WATER_QUALITY_READING_TYPES = ["chlorine_residual", "turbidity", "coliform", "lead_and_copper"]
BILLING_DIVERGENCE_THRESHOLD_PCT = 2.0


@dg.asset_check(
    asset=dg.AssetKey(["meter_reading_extract"]),
    blocking=True,
    description=(
        f"Fails the partition when its meter-read row count falls below the expected "
        f"per-day floor ({EXPECTED_METER_READING_ROW_COUNT_FLOOR:,}) -- would block dependent "
        "warehouse assets from computing on an incomplete day of reads."
    ),
)
def meter_reading_extract_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Maps to the brief's "high frequency data loads... optimization" pain:
    a utility can't bill accurately on an incomplete day of meter reads.
    """
    partition_key = context.partition_key
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"{partition_key}: completeness check passed (graph-first demo -- no row count "
            f"computed). In production, would fail below {EXPECTED_METER_READING_ROW_COUNT_FLOOR:,} rows."
        ),
        metadata={
            "expected_row_count_floor": EXPECTED_METER_READING_ROW_COUNT_FLOOR,
            "check_semantics": "row_count >= floor",
            "partition": partition_key,
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["water_quality_compliance_daily"]),
    blocking=True,
    description=(
        "Fails the partition when a required reading type is missing for the day -- would "
        "block the regulator-facing extract from computing on an incomplete compliance rollup."
    ),
)
def water_quality_compliance_daily_completeness(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps to the (industry-standard) EPA Safe Drinking Water Act
    compliance pain named in the brief's Data domains section.
    """
    partition_key = context.partition_key
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"{partition_key}: all {len(REQUIRED_WATER_QUALITY_READING_TYPES)} required reading "
            "types present (graph-first demo -- no lab data computed). In production, would fail "
            "on any missing required reading type for the day."
        ),
        metadata={
            "required_reading_types": REQUIRED_WATER_QUALITY_READING_TYPES,
            "check_semantics": "all required_reading_types present for partition",
            "partition": partition_key,
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["billing_accuracy_report"]),
    blocking=False,
    description=(
        f"Warns when billed usage diverges from raw meter delta by more than "
        f"{BILLING_DIVERGENCE_THRESHOLD_PCT}% for any account -- surfaced, not blocking."
    ),
)
def billing_accuracy_report_reconciliation(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps directly to the demo thesis: "which job touched this data and
    did it pass its checks." Warning, not blocking -- worth surfacing, not
    worth stopping billing over.
    """
    return dg.AssetCheckResult(
        passed=True,
        description=(
            "No accounts over the divergence threshold (graph-first demo -- no billing/meter "
            f"data computed). In production, would warn above {BILLING_DIVERGENCE_THRESHOLD_PCT}% "
            "divergence between billed usage and raw meter delta."
        ),
        metadata={
            "divergence_threshold_pct": BILLING_DIVERGENCE_THRESHOLD_PCT,
            "check_semantics": "abs(billed_usage - raw_meter_delta) / raw_meter_delta <= threshold",
        },
    )
