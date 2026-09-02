"""The three asset checks for the Trafigura demo, each mapped to a directive
in the brief's Asset checks section. Custom Python because assertion logic
is business logic, not something a component wraps -- registry search for a
declarative completeness/reconciliation check turned up nothing relevant in
the detroit-dwsd build (searched "asset check declarative yaml", "row count
completeness check", 2026-08-28, dagster-community-components-cli 0.8.15);
same gap applies here, not re-searched. Kept in one file rather than three:
each check is a handful of lines and none share code.

Graph-first fidelity means there's no real trade or exposure figure to
compute here -- every check always passes and reports the rule it would
enforce against real data in production. The talk track is what each check
catches when it's wired to a live feed, not a live number today.
"""

import dagster as dg

RECONCILIATION_TOLERANCE_PCT = 1.0
PRICE_FEED_STALENESS_THRESHOLD_MINUTES = 15


@dg.asset_check(
    asset=dg.AssetKey(["trade_capture_raw"]),
    blocking=True,
    description=(
        "Fails when any trade record is missing a valid counterparty_id or "
        "commodity_id -- would block every downstream position and exposure "
        "figure from computing on an unresolvable trade."
    ),
)
def trade_capture_raw_completeness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Maps to the brief's first Asset checks directive: every trade record
    has a valid counterparty_id and commodity_id before flowing downstream.
    """
    return dg.AssetCheckResult(
        passed=True,
        description=(
            "All trade records resolve to a valid counterparty_id and commodity_id "
            "(graph-first demo -- no trade data computed). In production, would fail "
            "on any trade record missing either reference."
        ),
        metadata={
            "check_semantics": "counterparty_id IS NOT NULL AND commodity_id IS NOT NULL for every row",
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["fact_credit_exposure_daily"]),
    blocking=True,
    description=(
        f"Fails the partition when exposure diverges from source trade capture by more "
        f"than {RECONCILIATION_TOLERANCE_PCT}% -- would block the risk dashboard from "
        "computing on an unreconciled exposure figure."
    ),
)
def fact_credit_exposure_daily_reconciliation(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps to the brief's second Asset checks directive: exposure figures
    reconcile against source trade capture within tolerance.
    """
    partition_key = context.partition_key
    return dg.AssetCheckResult(
        passed=True,
        description=(
            f"{partition_key}: exposure reconciles against source trade capture within "
            f"{RECONCILIATION_TOLERANCE_PCT}% (graph-first demo -- no exposure data computed). "
            f"In production, would fail above {RECONCILIATION_TOLERANCE_PCT}% divergence."
        ),
        metadata={
            "reconciliation_tolerance_pct": RECONCILIATION_TOLERANCE_PCT,
            "check_semantics": "abs(computed_exposure - trade_capture_derived_exposure) / trade_capture_derived_exposure <= tolerance",
            "partition": partition_key,
        },
    )


@dg.asset_check(
    asset=dg.AssetKey(["commodity_price_feed_raw"]),
    blocking=False,
    description=(
        f"Warns when the commodity price feed hasn't refreshed within "
        f"{PRICE_FEED_STALENESS_THRESHOLD_MINUTES} minutes of its expected cadence -- "
        "surfaced, not blocking."
    ),
)
def commodity_price_feed_raw_staleness(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Maps to the brief's third Asset checks directive: market data arrives
    within its expected window. Warning, not blocking -- a stale price is
    worth surfacing to a desk, not worth halting position marking over.
    """
    return dg.AssetCheckResult(
        passed=True,
        description=(
            "Feed refreshed within its expected window (graph-first demo -- no feed "
            f"timestamp computed). In production, would warn past "
            f"{PRICE_FEED_STALENESS_THRESHOLD_MINUTES} minutes of staleness."
        ),
        metadata={
            "staleness_threshold_minutes": PRICE_FEED_STALENESS_THRESHOLD_MINUTES,
            "check_semantics": "now() - feed_last_refreshed_at <= threshold",
        },
    )
