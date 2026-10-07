# One Definition, 170 Tenants

A Dagster demo for Skygen USA: ~170 per-client SQL Server databases moving to
Snowflake, Fivetran and dbt, published to Sigma. One definition, partitioned by
tenant x day, gives per-tenant lineage, quality gates, freshness and selective
reruns -- without 170 configs. **Demo mode: synthetic data, no credentials.**

## The graph (16 assets)

| Group | Assets | Real component |
|---|---|---|
| `fivetran_ingest` | `client_landing/{dental_claims, vision_claims, member_eligibility}` | `dagster_fivetran.FivetranAccountComponent` (subclassed for demo mode) |
| `dbt_ingest` -> `dbt_transform` -> `dbt_published` -> `dbt_mart` | 9 real dbt Core models | `dagster_dbt.DbtProjectComponent` (subclassed) |
| `sigma_reporting` | `Client_SLA_Scorecard`, `Client_Utilization_Overview` | `dagster_sigma.SigmaComponent` (subclassed for demo mode) |
| `legacy_ssis_estate` | `legacy_client_database_nightly_load` -> `legacy_client_sla_report_export` | community `ssis_workspace` (subclassed for demo mode) |

The SSIS group sits beside the new path on purpose and is **not** wired into
Fivetran -> dbt -> Sigma lineage: whether SSIS survives into the future state is
open. Snowflake is a kinds badge (DuckDB executes); the dbt `live` profile target is the real path.

**Partitions:** `date x tenant` (`MultiPartitionsDefinition`) on every Fivetran and
dbt asset. 20 demo tenants (`tenant_001`...) stand in for ~170. Adding a tenant is one more entry in
`TENANTS` (`src/skygen/partitions.py`); no new asset or component instance.

## Checks (all green)
- `landed_row_count_completeness` (**blocking**, one definition over all 3 landed tables): the "98% row count" pain. dbt refuses to build on an incomplete landing.
- `published_claims_completeness` (**blocking**): gates the marts/Sigma on a complete published layer.
- `client_sla_data_ready` (warning): tenant data ready before its SLA deadline.
- 14 dbt-native tests, surfaced as checks.

Also: native freshness policy on `mart_client_sla_scorecard`; eager automation gated on blocking checks on every dbt asset; one nightly schedule (`skygen_nightly_landing_schedule`, 01:00 UTC); polling sensors on Fivetran and SSIS.

## Three buckets
- **In code:** the graph above, partitions, checks, freshness, automation, schedule, sensors, metadata.
- **Dagster+ (demonstrate, not built):** Slack/email SLA alerting, branch deployments and CI/CD, catalog and lineage UI, RBAC/audit, restart from failure, selective rerun/backfill UI.
- **Conversation only:** Terraform vs. Dagster (Dagster has a Terraform provider), credit estimate at 170 tenants, POC scope (2-3 tenants, Fivetran + dbt), phData tooling, PowerShell/CI tooling. Nothing built.

## Assumptions (not in the brief)
- One Fivetran connector with tenant as a partition dimension. Real Fivetran is typically one connector per client DB; the same component handles that with `connector_selector`.
- Tenants, tables and columns are generic dental/vision benefits shapes; SLA deadline 06:00 UTC next day, schedule 01:00 UTC, freshness 26h warn / 36h fail are this build's own.
- Sigma is an external-spec integration; it has no observation sensor (see `component-feedback/`).
- The AE doc's staged failure + Fivetran resync step is deliberately not built (house rules). Talk it through against the green graph.

## Run it
```bash
uv sync && source .venv/bin/activate
dg dev                       # live demo, locally
python validate_e2e.py       # full end-to-end gate
```
Zero setup: no env vars; the demo warehouse is `src/skygen/demo_data/skygen.duckdb`, created on first use.

**Local vs Dagster+:** Serverless storage is ephemeral (fresh container per run). Run the live walkthrough locally with `dg dev`. In Dagster+ the deployment proves the project is real (loads, graph renders); if you materialize there, select the whole chain in **one run** so landing and dbt share a container.

## Going live
Set `demo_mode: false` and add real credentials in `defs/fivetran_ingest/defs.yaml` (`workspace`), `defs/reporting/defs.yaml` (`organization`), `defs/legacy_ssis_estate/defs.yaml` (`workspace`), and `SKYGEN_DBT_TARGET=live` plus Snowflake env vars for dbt.
