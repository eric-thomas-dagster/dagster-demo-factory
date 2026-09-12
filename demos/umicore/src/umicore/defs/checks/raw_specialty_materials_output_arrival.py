"""Blocking freshness/arrival check on the upstream business-unit asset in
Anthony's 3-job sequential chain example -- the literal fix for "if the
first [of three sequential data projects] runs long, the other two sit
stale until the next scheduled window, with no automatic recovery."

This check gates `stg_specialty_materials` and, transitively via eager
automation, `fct_cross_bu_consolidated_ledger` and
`executive_reporting_extract` -- the two downstream legs of the chain --
from ever computing against a day where specialty_materials' own raw feed
didn't actually land.

No registry component covers a declarative arrival check against an
arbitrary DuckDB query -- asset-check assertion logic is business logic,
one of the justified `.py` files in `defs/checks/` (see README).
"""

import dagster as dg

from umicore.demo_data.warehouse import demo_duckdb_path

MINIMUM_EXPECTED_ROWS = 20


@dg.asset_check(
    asset=dg.AssetKey(["specialty_materials", "raw_specialty_materials_output"]),
    blocking=True,
    description=(
        f"Fails when fewer than {MINIMUM_EXPECTED_ROWS} specialty-materials output rows "
        "landed for the day -- blocks stg_specialty_materials and, via eager automation, "
        "fct_cross_bu_consolidated_ledger and executive_reporting_extract from computing "
        "on a partial/late-arriving feed."
    ),
)
def raw_specialty_materials_output_arrival(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        row_count = conn.execute("select count(*) from raw.specialty_materials_output").fetchone()[0]
    finally:
        conn.close()

    passed = row_count >= MINIMUM_EXPECTED_ROWS
    return dg.AssetCheckResult(
        passed=passed,
        description=(
            f"{row_count} specialty-materials output rows on file, at/above the floor of "
            f"{MINIMUM_EXPECTED_ROWS}."
            if passed
            else f"Only {row_count} specialty-materials output rows on file against a floor "
            f"of {MINIMUM_EXPECTED_ROWS} -- the feed looks late or incomplete for this run."
        ),
        metadata={"row_count": row_count, "minimum_expected_rows": MINIMUM_EXPECTED_ROWS},
    )
