"""The three asset checks for the E.ON Sverige demo, each mapped to a pain
named in the brief. Custom Python because assertion logic is business logic,
not something a component wraps -- the same registry gap recorded for the
City of Detroit DWSD build (`component-feedback/2026-08-28-graph-first-assets.md`
covers the sibling "no-op asset" gap; no separate check-declaration component
exists either). Kept in one file rather than three: each check is a handful
of lines and none share code, so three files would be pure file-count
overhead in a `defs/` tree that's meant to be predominantly YAML.

Graph-first fidelity means there's no real meter reading, load value, or
audit record to compute here -- every check always passes and reports the
threshold or required-field set it would enforce against real data in
production. The talk track is what each check catches once it's wired to a
live feed, not a live number today.
"""

import dagster as dg

REQUIRED_AUDIT_METADATA_FIELDS = ["produced_by_run_id", "produced_at", "source_partition", "upstream_check_status"]
GRID_LOAD_MIN_MW = 0.0
GRID_LOAD_MAX_MW = 5_000.0


@dg.asset_check(
    asset=dg.AssetKey(["raw_meter_reads"]),
    blocking=True,
    description=(
        "Fails the partition when an expected meter ID for the partition's grid "
        "bidding zone reports no reading -- would block validated_meter_reads and "
        "everything downstream from computing on an incomplete day of reads."
    ),
)
def meter_reads_completeness_check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Maps to the brief's "step-change in telemetry volume" pain: the
    NB-IoT smart-meter rollout means a much larger set of meters can go
    silent for a zone on a given day.
    """
    partition_key = context.partition_key
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"{partition_key}: every expected meter ID reported a reading (graph-first demo -- "
            "no meter list computed). In production, would fail on any expected meter ID missing "
            "from the day's reads for its zone."
        ),
        metadata={
            "check_semantics": "every expected_meter_id in raw_meter_reads for (date, region)",
            "partition": partition_key,
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["raw_grid_load_telemetry"]),
    blocking=True,
    description=(
        f"Fails when a reported load value falls outside a physically plausible range "
        f"({GRID_LOAD_MIN_MW}-{GRID_LOAD_MAX_MW} MW) -- would block grid_load_hourly from "
        "computing on a telemetry glitch."
    ),
)
def grid_load_range_check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Maps to the brief's "criticality of grid telemetry" pain -- a bad
    sensor reading feeding straight into an hourly rollup is a
    grid-operations risk, not just a data-quality one.
    """
    partition_key = context.partition_key
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"{partition_key}: all reported load values within "
            f"{GRID_LOAD_MIN_MW}-{GRID_LOAD_MAX_MW} MW (graph-first demo -- no load values "
            "computed). In production, would fail on any value outside this range."
        ),
        metadata={
            "grid_load_min_mw": GRID_LOAD_MIN_MW,
            "grid_load_max_mw": GRID_LOAD_MAX_MW,
            "check_semantics": "min_mw <= load_value <= max_mw",
            "partition": partition_key,
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["customer_switching_extract"]),
    blocking=True,
    description=(
        f"Fails when any of the {len(REQUIRED_AUDIT_METADATA_FIELDS)} audit fields EU "
        "2026/855 implies (who/what produced this extract, when, against which check "
        "results) is missing -- would block switching_data_audit_log from computing on "
        "an unauditable extract."
    ),
)
def switching_extract_audit_completeness_check(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps directly to the demo thesis: EU 2026/855's transparent,
    auditable data-access requirement for customer-switching data. Made
    blocking (the brief allows warning-only here, but prefers blocking
    given two other blocking checks already exist) because an unauditable
    extract is exactly the failure mode the regulation exists to prevent.
    """
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"All {len(REQUIRED_AUDIT_METADATA_FIELDS)} required audit fields present "
            "(graph-first demo -- no extract computed). In production, would fail on any "
            "missing required audit field."
        ),
        metadata={
            "required_audit_fields": REQUIRED_AUDIT_METADATA_FIELDS,
            "check_semantics": "all required_audit_fields present on customer_switching_extract",
        },
    )
