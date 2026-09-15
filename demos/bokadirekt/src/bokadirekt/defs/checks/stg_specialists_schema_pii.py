"""Warning-severity schema/PII shape check on the specialist roster.

Ties to the GDPR baseline noted in the brief (Bokadirekt is a Swedish
company processing EU consumer/specialist personal data): asserts the
expected columns are present and no unexpected PII-shaped column (a
national ID, email, or phone number) has been added upstream. A warning,
not a blocker, because a shape drift here is worth a human look, not an
automatic halt of the entity layer.
"""

import dagster as dg

from bokadirekt.demo_data.warehouse import demo_duckdb_path

EXPECTED_COLUMNS = {"specialist_id", "specialist_name", "region", "service_category", "onboarded_date"}
DISALLOWED_PII_PATTERNS = ("ssn", "personnummer", "national_id", "email", "phone", "address")


@dg.asset_check(
    asset=dg.AssetKey(["staging", "stg_specialists"]),
    blocking=False,
    description=(
        "Warns if the specialist roster's expected columns go missing, or if "
        "a PII-shaped column (national ID, email, phone, address) shows up "
        "unannounced -- the GDPR guardrail for this feed."
    ),
)
def stg_specialists_schema_pii(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    import duckdb

    conn = duckdb.connect(demo_duckdb_path(), read_only=True)
    try:
        columns = {
            row[0].lower()
            for row in conn.execute(
                "select column_name from information_schema.columns "
                "where table_schema = 'main_staging' and table_name = 'stg_specialists'"
            ).fetchall()
        }
    finally:
        conn.close()

    missing = EXPECTED_COLUMNS - columns
    unexpected_pii = {col for col in columns for pattern in DISALLOWED_PII_PATTERNS if pattern in col}

    passed = not missing and not unexpected_pii
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"missing expected columns: {sorted(missing) or 'none'}; "
            f"unexpected PII-shaped columns: {sorted(unexpected_pii) or 'none'}."
        ),
        metadata={
            "missing_columns": sorted(missing),
            "unexpected_pii_columns": sorted(unexpected_pii),
            "observed_columns": sorted(columns),
        },
    )
