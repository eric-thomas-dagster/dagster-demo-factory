"""End-to-end validation of the Trafigura Group demo ("One Ledger, Every
Desk").

`dg launch --assets '*'` exits immediately on any partitioned asset ("Asset
has partitions, but no '--partition' option was provided") -- two of this
project's ten assets (`fact_trade_position_daily`,
`fact_credit_exposure_daily`) are daily-partitioned, so this script is the
harness `scripts/validate_demo.sh` requires instead. It proves:

1. The eight unpartitioned assets (market data, trade capture, the two
   warehouse dims, and the reporting dashboard) execute standalone.
2. The two daily-partitioned risk facts materialize cleanly across two
   validation dates, including their blocking reconciliation check --
   `fact_trade_position_daily` depends on the unpartitioned dims via a plain
   `deps=` edge, so this also proves that dependency resolves without
   requiring the upstream dims to already be materialized in this run.
3. All three asset checks run and pass.

Per the brief and house rules this is a graph-first, always-green demo:
every asset body is a no-op (no synthetic data, no planted anomaly), so
there's no row count to print. What "determinism across runs" means here
instead is structural: the same asset count, check count, and pass/fail
outcome every time -- asserted below rather than assumed.

Run with: `python validate_e2e.py`
"""

import sys

import dagster as dg

from trafigura.definitions import defs as defs_lazy

UNPARTITIONED_KEYS = [
    dg.AssetKey("commodity_price_feed_raw"),
    dg.AssetKey("fx_rate_feed_raw"),
    dg.AssetKey("freight_rate_feed_raw"),
    dg.AssetKey("trade_capture_raw"),
    dg.AssetKey("counterparty_reference_data_raw"),
    dg.AssetKey("dim_counterparty"),
    dg.AssetKey("dim_commodity"),
    dg.AssetKey("power_bi_trading_risk_dashboard"),
]

PARTITIONED_KEYS = [
    dg.AssetKey("fact_trade_position_daily"),
    dg.AssetKey("fact_credit_exposure_daily"),
]

ALL_ASSET_COUNT = len(UNPARTITIONED_KEYS) + len(PARTITIONED_KEYS)  # 10

VALIDATION_DATES = ["2026-08-31", "2026-09-01"]

CHECK_NAMES = (
    "trade_capture_raw_completeness",
    "fact_credit_exposure_daily_reconciliation",
    "commodity_price_feed_raw_staleness",
)

_failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {message}")
    if not condition:
        _failures.append(message)


def run(
    definitions: dg.Definitions,
    instance: dg.DagsterInstance,
    keys: list[dg.AssetKey],
    partition_key: str | None,
    label: str,
):
    job = definitions.resolve_implicit_job_def_def_for_assets(keys)
    if job is None:
        raise RuntimeError(f"no job resolved for {label}")
    result = job.execute_in_process(
        instance=instance,
        partition_key=partition_key,
        asset_selection=keys,
        raise_on_error=False,
    )
    if not result.success:
        raise RuntimeError(f"materialize failed: {label} partition={partition_key}")
    return result


def main() -> int:
    definitions = defs_lazy()
    instance = dg.DagsterInstance.ephemeral()

    print(f"==> Structural check: {ALL_ASSET_COUNT} assets declared")
    asset_graph = definitions.get_repository_def().asset_graph
    check(
        len(asset_graph.get_all_asset_keys()) == ALL_ASSET_COUNT,
        f"asset graph has exactly {ALL_ASSET_COUNT} assets",
    )

    seen_checks: set[str] = set()

    print("\n==> Unpartitioned chain (market data, trade capture, dims, dashboard)")
    result = run(definitions, instance, UNPARTITIONED_KEYS, partition_key=None, label="unpartitioned chain")
    check(True, f"materialized {len(UNPARTITIONED_KEYS)} unpartitioned assets")
    for evaluation in result.get_asset_check_evaluations():
        seen_checks.add(evaluation.check_name)
        check(evaluation.passed, f"check '{evaluation.check_name}' passed")

    print("\n==> Partitioned chain (trade position + credit exposure)")
    for date in VALIDATION_DATES:
        result = run(definitions, instance, PARTITIONED_KEYS, partition_key=date, label=f"partitioned chain {date}")
        check(True, f"{date}: materialized {len(PARTITIONED_KEYS)} partitioned assets")
        for evaluation in result.get_asset_check_evaluations():
            seen_checks.add(evaluation.check_name)
            check(evaluation.passed, f"{date}: check '{evaluation.check_name}' passed")

    print("\n==> All expected checks ran")
    for name in CHECK_NAMES:
        check(name in seen_checks, f"check '{name}' evaluated at least once")

    print()
    if _failures:
        print(f"FAILED: {len(_failures)} assertion(s) did not hold:")
        for f in _failures:
            print(f"  - {f}")
        return 1

    print("PASSED: all assets materialized, all checks evaluated and green.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
