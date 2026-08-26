# Stellantis Financial Services — Dagster demo

**Thesis:** Dagster sits on top of the Microsoft Fabric pipelines SFS is already
migrating their ~700 SSIS packages into — it orchestrates, observes, checks,
and enables partition-scoped recovery on that estate, without asking SFS to
rebuild a single package. This is an **orchestrate-existing-workloads** demo,
not a from-scratch transformation build (see `briefs/2026-08-26-stellantis-financial-services.md`
for the full brief, including why an earlier from-scratch dbt version of this
demo was scrapped and rebuilt this way).

## The asset graph

Vendor files (dealer originations, lease originations, payment transactions,
dealer floorplan advances, credit-bureau pulls) land daily and flow through
bronze → silver → gold → the Power BI refresh, 17 assets total:

- **Bronze** (`raw/*`) — one trigger-and-observe asset per vendor feed. Each
  one triggers the Fabric Data Pipeline that has replaced (or is replacing)
  the equivalent SSIS package.
- **Silver** (`staging/*`) — conformed staging tables and two dimensions
  (`dim_dealer`, `dim_borrower`).
- **Gold** (`marts/*`) — the loan-level portfolio tape
  (`fact_loan_portfolio`), a daily delinquency snapshot, the money-shot
  **`abs_pool_eligibility`** table SFS's 2026 ABS securitization calendar
  depends on, a GL reconciliation summary, and a per-borrower `customer_360`.
- **Reporting** (`reporting/*`) — a single trigger asset representing the
  Fabric-native Power BI dashboard refresh.

Every asset injects one shared resource, `FabricPipelineResource`
(`components/fabric_resource.py`), and calls `fabric.trigger_and_wait(...)` —
Dagster is never recomputing SFS's transformation logic, it is triggering and
observing the Fabric pipeline that already does.

`raw_dealer_floorplan_feed` is the one asset with a genuine second partition
dimension (`date` × `dealer_group`, four regional dealer groups) and a real
retry policy — floorplan advances arrive as separate per-region SFTP batches
from ~240 individual dealer relationships, which is a real source of
transient failure the other single-vendor feeds don't share. `dim_dealer`
depends on all four `dealer_group` partitions for a date via a
multi-to-single-dimension partition mapping.

## Run-of-show

1. **Open `dg dev`, land on the asset graph.** Everything is green. Point out
   the `fabric` / `azure` kind badges on bronze/silver/gold, and `powerbi` on
   the reporting asset — "this reads as your stack because it *is* your
   stack; we're not asking you to look at DuckDB icons."
2. **Click into `abs_pool_eligibility`.** Show the freshness policy (9am ET
   deadline) and the blocking `abs_pool_eligibility_reconciliation` check —
   "this is the audit-ready loan tape your ABS deals depend on; it can't
   silently be short a row or ship a contract with no credit score."
3. **Select the `raw_dealer_floorplan_feed` asset, partition
   `2026-08-22 | midwest_dealers`.** Materialize it. The blocking
   `raw_dealer_floorplan_feed_completeness` check fails — one advance in that
   batch is missing `loan_id`. Downstream Fabric pipeline triggers for that
   partition never fire. **This is the planted anomaly.**
4. **Run `make simulate-correction`** — "the dealer resends the corrected
   file. That's the entire recovery procedure on our end."
5. **Rematerialize just that one partition.** It succeeds. Narrate: "we just
   recovered one dealer group's one day of data — not the other three dealer
   groups, not any other date, and not the other 699 packages' worth of
   pipeline. That's your replay/backfill answer."
6. **Show the schedules** (`bronze_ingestion_schedule`,
   `dealer_floorplan_ingestion_schedule`) and the automation conditions on
   silver/gold — "ingestion runs on your actual file-drop deadline; everything
   downstream recomputes itself the moment new data lands, no cron chain to
   maintain."
7. **Mention `portfolio_pipeline_failure_sensor`** (Microsoft Teams,
   `dagster-msteams`) — "this posts to the channel your team already lives
   in. It's off by default in the demo; flip it on with a real webhook URL
   and it's live."

## What's mocked vs. real

| Layer | Demo mode | Real mode |
|---|---|---|
| Fabric pipeline trigger/poll | `FabricPipelineResource.trigger_and_wait` returns a canned `Completed` status locally | Calls the real `fabric_workspace_resource` community-registry client (`trigger_item_run` + `wait_for_run` against the Fabric REST API) |
| Vendor-file contents | Deterministic synthetic generators (`demo_data/generators.py`), seeded so every run produces identical numbers | Whatever the Fabric pipeline actually lands in the Lakehouse/Warehouse |
| Warehouse | Local DuckDB file (`demo_data/demo.duckdb`), created on first use | Wherever SFS's Fabric pipelines actually write (Lakehouse Delta tables, Fabric Data Warehouse) |
| Teams alerting | Configured, sensor stopped by default, no webhook required | Set `MSTEAMS_WEBHOOK_URL` and start the sensor |

**To go live:** edit `src/stellantis_financial_services/defs/resources/fabric/defs.yaml` —
set `demo_mode: false`, `workspace_id` to SFS's real Fabric workspace GUID,
and point `tenant_id_env_var` / `client_id_env_var` / `client_secret_env_var`
at real service-principal credentials. No asset code changes required —
every asset key, partition, check, and dependency edge is identical in both
modes, per `templates/demo_mode_pattern.py`.

## Local vs. Dagster+

**Give the live walkthrough locally with `dg dev`.** The recovery sequence in
step 3–5 above spans two runs against the same local DuckDB file, and
Dagster+ Serverless storage is ephemeral (each run is a fresh container, so a
local DuckDB path does not persist between runs there). The Dagster+
deployment is the proof point that the code location loads and the graph is
real — not the place to click through the recovery sequence.

## Zero setup

```
git clone <repo>
cd demos/stellantis-financial-services
uv sync
source .venv/bin/activate
dg dev
```

No environment variables to set, no credentials, no manual file creation.
The planted anomaly on `raw_dealer_floorplan_feed` (`2026-08-22 |
midwest_dealers`) is pre-seeded — the first materialize of that partition
reproduces it automatically.

`make reset-demo` restores a clean slate (deletes the local DuckDB warehouse
and the floorplan correction state) between run-throughs.

## Validation

```
python validate_e2e.py
```

Materializes the full 14-day demo window across every asset and check,
confirms the planted anomaly genuinely blocks downstream, and proves recovery
is a plain rematerialize.

## Feature coverage

| Capability | In this build |
|---|---|
| Partitions | `DailyPartitionsDefinition` on every asset; `MultiPartitionsDefinition` (date × dealer_group) on `raw_dealer_floorplan_feed`, mapped to date-only downstream via `MultiToSingleDimensionPartitionMapping` |
| Asset checks | 4 total — 3 blocking (`raw_loan_originations_completeness`, `raw_dealer_floorplan_feed_completeness`, `abs_pool_eligibility_reconciliation`), 1 warning (`payment_transactions_reconciliation`) |
| Transformation layer | Fabric trigger-and-observe (native pattern for SFS's stack) — **no dbt**, since nothing in the brief's AE notes or public research indicates SFS runs dbt |
| Freshness policies | `fact_delinquency_snapshot`, `abs_pool_eligibility` — 9am ET cron deadline |
| Retry policy | `raw_dealer_floorplan_feed` only — the one genuinely flaky source (per-dealer SFTP drops). Deliberately skipped elsewhere: a decorative retry on a deterministic single-vendor feed invites a question we'd lose |
| Automation conditions | `AutomationCondition.eager()` on every silver/gold/reporting asset |
| Schedules | `bronze_ingestion_schedule` (6am ET), `dealer_floorplan_ingestion_schedule` (6:30am ET) — both stopped by default, matching the zero-setup rule |
| Asset metadata | Row counts, dollar totals, pool-eligible counts, Fabric run status on every asset |
| Kinds | `fabric` + `azure` on bronze/silver/gold, `powerbi` on reporting — no `duckdb` badges anywhere |
| Alerting | `dagster-msteams` run-failure sensor, stopped by default, configured with a placeholder webhook |

## Explicitly out of scope

Rewriting SFS's SSIS/stored-procedure transformation logic in a new engine.
A real embedded Power BI workspace integration (the reporting asset is a
single refresh trigger). Multi-tenant RBAC/SSO, branch deployments, and any
GLBA-compliance tooling itself. See the brief's "Explicitly out of scope"
section for the full list.
