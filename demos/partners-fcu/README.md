# Partners Federal Credit Union — Dagster demo

## "Dependencies, Not Duct Tape"

Partners FCU already piloted Dagster OSS about a year ago and let it stall
to competing priorities, not to a technical objection. This meeting is
their own "Pro vs. OSS" comparison, with three concrete, named asks:
alerting (untested in the OSS pilot and the thing they "really care
about"), quality checks, and partitioned assets.

## The pitch

Jason (BI technical lead, ran the original OSS pilot), Mark (Enterprise
Architect, ex-Airflow), Srinath (Data Architect), and Len (implementation
consultant) are evaluating Dagster+ specifically over self-hosting. Their
own words: "always building workarounds to get jobs to run in the right
order and have dependencies running properly." This demo answers that pain
directly — a real dbt Core project against their real source (SQL Server),
with a blocking completeness check that refuses to compute downstream on
an incomplete daily feed, daily partitions on the fact layer, and a live
Dagster+ alerting policy wired to that same check on screen.

**Every external system in the graph runs through the real, corresponding
Dagster integration or registry component** (native `dagster-dbt`, the
registry's `mssql_io_manager`), with the network boundary mocked, not a
home-made stand-in — see "Integration mapping" below.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required.** `PARTNERS_FCU_DEMO_DUCKDB_PATH` defaults to a project-relative
DuckDB file, created automatically the moment Dagster's definitions load —
see "Fidelity" below.

## Integration mapping — every system, its real component

| System | Component | Rung | Escalation reasoning |
|---|---|---|---|
| **SQL Server** (ingestion, their real current source) | `dagster_component_templates.MSSQLIOManagerComponent`, subclassed as `DemoMSSQLIOManagerComponent` | 2 (registry), subclassed per rung 3 | `dagster-component search "sql server" --json` found `mssql_io_manager` directly (score 248). Subclass swaps the entire IO manager resource for `dagster_duckdb_pandas.DuckDBPandasIOManager` in demo mode — see `components/demo_mssql_io_manager.py`. |
| **dbt Core** (transformation, real and currently running) | `dagster_dbt.DbtProjectComponent`, subclassed as `PartnersFcuDbtComponent` | 1 (native), subclassed for kind-badge only | Runs for real in both demo and live mode — no I/O seam needed. Subclass only overrides `get_asset_spec` to badge `dbt`+`snowflake` instead of the manifest-derived `duckdb`. |
| **Snowflake** (warehouse, "likely adding" — not decided) | `kinds={"snowflake"}` badge on the dbt layer | n/a | Engine is DuckDB in demo mode; badge reflects the AE's own forward-looking framing, **flagged as an assumption** — see below. No real Snowflake credentials or I/O anywhere in this build. |
| **BI tool** (reporting) | none — no kind badge | n/a | No BI tool is named anywhere in the AE notes or public research. `member_activity_reporting_extract` is a plain `@asset` with no kind, per house rules against guessing Power BI/Tableau/Looker. |

## Fidelity: graph-first for ingestion, dbt runs for real

Per the brief, the SQL Server ingestion layer is graph-first: each raw
asset's demo-mode execution body writes a handful of deterministic,
plausible-cardinality stub rows (not a full synthetic-data-generation
apparatus) through the real `mssql_io_manager` registry component,
DuckDB-backed in demo mode. **dbt is the exception, by design** — Partners
FCU's own named ask is "quality checks," and dbt's own tests are the
direct, no-extra-plumbing answer, so the dbt layer runs a real, small dbt
Core project (native `dagster-dbt`) against DuckDB, with real
`not_null`/`unique` tests surfaced as Dagster asset checks automatically.

This is a deliberate, small departure from the brief's own literal
"nothing in the brief demands specific numbers on screen" framing: Axis 1
(CLAUDE.md — every external system gets real I/O through its real
component) requires the raw layer to actually write rows somewhere, so the
blocking completeness checks and the dbt sources have something real to
query. The rows themselves are trivial and deterministic — this is not a
data-backed-fidelity build.

Flipping to Partners FCU's real SQL Server is a `demo_mode: false` +
`MSSQL_URL` change in `defs/resources/defs.yaml` (same asset keys, specs,
partitions, and YAML schema either way — `templates/demo_mode_pattern.py`).
Flipping to a real Snowflake warehouse is a `dbt_project/profiles.yml`
target change (`live` output already stubbed) — same dbt project, same
tests, same asset graph.

## The asset graph

Ten assets across four groups, one lineage graph:

- **`ingestion`** (3 assets, badged `mssql`, real `mssql_io_manager`
  registry component subclass) — `raw_member_transactions`,
  `raw_deposit_accounts`, `raw_loan_originations`. Daily time-partitioned —
  their own explicit ask, "talk through partitioned assets." One
  `WarehouseTableAssetsComponent` instance with an explicit `assets:`
  mapping table in `defs/ingestion/defs.yaml` — adding Partners FCU's next
  SQL Server source table is one more entry, never a new component
  instance.
- **`dbt`** (6 assets, badged `dbt`+`snowflake`, real dbt Core) —
  `staging/stg_member_transactions`, `staging/stg_deposit_accounts`,
  `staging/stg_loan_originations`, `marts/dim_member` (unpartitioned),
  `marts/fct_daily_member_activity`, `marts/fct_loan_portfolio_summary`
  (both daily partitioned).
- **`reporting`** (1 asset, no kind badge, plain `@asset`) —
  `member_activity_reporting_extract`, downstream of
  `fct_daily_member_activity`. No BI tool is named in the brief.

**Note on asset count:** the brief's Build directives name "~11 assets"
and enumerate exactly ten by name (3 ingestion + 6 dbt + 1 reporting). This
build follows the explicit named list — 10 total.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `raw_member_transactions_completeness` | `raw_member_transactions` | **Blocking** | Fewer than 100 member transactions on file for the day — direct reversal of "workarounds to get jobs to run in the right order... dependencies running properly." |
| `raw_loan_originations_completeness` | `raw_loan_originations` | **Blocking** | Fewer than 5 loan originations on file for the day. |
| 13 dbt-native `not_null`/`unique` tests | `stg_member_transactions`, `stg_deposit_accounts`, `stg_loan_originations`, `dim_member`, `fct_daily_member_activity`, `fct_loan_portfolio_summary` | Mixed (dbt default) | Missing/duplicate keys — auto-surfaced as Dagster asset checks, zero extra Dagster code, the direct answer to "quality checks." |
| `fct_daily_member_activity_reconciliation` | `fct_daily_member_activity` | Warning | The daily fact's summed `transaction_count` diverging from raw ingested rows — the aggregation step silently dropping or duplicating. |

All checks compute genuine results against the stub/dbt data (not
hardcoded passes) — `validate_e2e.py` asserts they evaluate to `True`, not
just that they ran. Per house rules, everything stays green: no planted
failure anywhere in this build.

## Freshness, automation, and schedule

- **Freshness policy**: `fct_daily_member_activity` (fail at 24h, warn at
  18h) — the asset Partners FCU's BI team would page someone about.
- **Automation conditions**: `AutomationCondition.eager()` on every dbt
  asset; `stg_member_transactions` and `stg_loan_originations`
  additionally gate on `all_deps_blocking_checks_passed()` — the direct
  reversal of the brief's named pain: propagation stops at the blocked
  asset instead of a hand-built ordering workaround.
- **Schedule**: `partners_fcu_morning_marts_schedule` computes the prior
  day's two daily facts at 06:00 America/New_York. **Assumption**: no
  cutover time or timezone is named in the brief.
- **Retry policies**: none. Per house rules, a decorative retry on a
  deterministic stub source invites a question we'd lose; nothing here is
  genuinely flaky.
- **Observation sensors**: none. `mssql_io_manager` is a plain IO manager,
  not a workspace-style component with a polling-sensor field — there is
  no externally-triggered-run concept for a SQL Server table the way
  there is for a Fivetran sync or an ADF pipeline.

## Three buckets

1. **Implemented in code**: the ten-asset graph (SQL Server ingestion via
   the real `mssql_io_manager` registry component, real dbt Core with
   native tests plus three custom checks), one freshness policy, eager
   automation (two check-gated), the morning schedule.
2. **Handled by Dagster+, demonstrated not built**: **native alerting**
   (Slack/email/PagerDuty) on check failures — the headline ask, wired
   live to the blocking completeness check in the walkthrough, never
   hand-rolled. Restart/rerun-from-point-of-failure — the direct fix for
   needing manual job-ordering workarounds today. Branch deployments —
   dev/test isolation, ties to the NCUA/GLBA vendor-risk framing. RBAC,
   viewer licenses, run history, asset health and lineage UI — the
   concrete "vs. OSS" answer across the board.
3. **Conversation only, nothing built**: the Snowflake migration itself
   (kind badge reflects intent, no real build); any ticketing/webhook
   integration (none named); the broader BI-replatform roadmap beyond this
   pipeline.

## `defs/` file count

**4 YAML, 7 Python** (plus boilerplate `defs/__init__.py`). By file count
Python is the majority here, but the ratio that actually matters — which
files *define assets* — runs the other way: **9 of 10 assets are
YAML-instantiated** (3 ingestion + 6 dbt), and only one
(`member_activity_reporting_extract`) is a hand-written Python asset,
justified below (same framing used by demos/rvu-tempcover). The remaining
six Python files are small, non-asset support code, each justified
individually:

- `defs/ingestion/defs.yaml` — 3 assets, `WarehouseTableAssetsComponent`
  with an explicit `assets:` mapping table.
- `defs/resources/defs.yaml` — the `mssql_io_manager` resource,
  `DemoMSSQLIOManagerComponent`.
- `defs/transformation/staging/defs.yaml`,
  `defs/transformation/marts_daily/defs.yaml` — the whole dbt layer (6
  assets), `PartnersFcuDbtComponent` instantiated twice (once per
  partitioning need — unpartitioned staging/dim vs. daily-partitioned
  facts).
- `defs/reporting/member_activity_reporting_extract.py` — **justified**:
  no BI tool is named anywhere in the brief, so there is no external
  system to route through a component; a one-object plain `@asset` is the
  right size for this Axis-2 rollup.
- `defs/transformation/staging/template_vars.py`,
  `defs/transformation/marts_daily/template_vars.py` — **justified**:
  Jinja (YAML template expressions) has no `&` operator and `dg`'s
  template scope doesn't expose `AutomationCondition`/`FreshnessPolicy`
  composition directly; exposing the composed Python values as
  `@dg.template_var`s is the documented sanctioned pattern.
- `defs/checks/raw_member_transactions_completeness.py`,
  `defs/checks/raw_loan_originations_completeness.py`,
  `defs/checks/fct_daily_member_activity_reconciliation.py` —
  **justified**: asset-check assertion logic is business logic; no
  registry component covers a declarative completeness/reconciliation
  check against an arbitrary DuckDB query.
- `defs/automation/morning_marts_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't
  express a specific hour alongside a `partitions_def`, so the
  one-function native `build_schedule_from_partitioned_job` call is used
  directly instead.

`components/demo_mssql_io_manager.py`, `components/warehouse_table_assets.py`,
`components/dbt_project.py`, `components/partitions.py`, and
`demo_data/warehouse.py` are components/support code, not `defs/` — only
`defs/` is measured for the YAML-first ratio.

## Registry search record

Per the brief's "Community components to search for" list and CLAUDE.md's
escalation ladder (at least three searches before writing custom code):

```
dagster-component search "sql server" --json
```
First and best hit: `mssql_io_manager` (score 248, `io_manager` category,
tagged `mssql`/`sqlserver`/`sql`) — "Persist DataFrames to a Microsoft SQL
Server schema, one table per asset." Used directly (rung 2), subclassed
per rung 3 for the demo-mode network-boundary seam. Also returned
`mssql_resource` (a bare connection resource, less complete than the IO
manager for this use case) and `ssis_workspace` (a different system, not
named in the brief).

No custom component was written for this build — nothing fell through to
rung 4, so no `component-feedback/` entry was needed.

## Which parts run where

The dbt layer's DuckDB file is local — it exists for the lifetime of one
process (or one Dagster+ Serverless container; **Serverless storage is
ephemeral**, so a fresh container starts with an empty warehouse each run).
**Give the interactive demo locally with `dg dev`** — the blocking check /
alerting walkthrough only persists across materializations there. Treat
the deployed Dagster+ code location as proof the project is real and loads
correctly, not as a place to run a multi-run recovery sequence.

## Assumptions

- **Timezone (`America/New_York`) and the 06:00 schedule hour** — no
  headquarters location or cutover time is named in the brief; Partners
  FCU is a US credit union, so Eastern is the most plausible default.
- **Freshness thresholds (24h fail / 18h warn)** on `fct_daily_member_activity`
  — no real numbers exist in the brief; this build's own plausible
  default, matching the convention used by demos/rvu-tempcover.
- **`kinds={"snowflake"}` on the dbt layer** — reflects the AE's own
  "likely also adding Snowflake" framing, not a decided target. No real
  Snowflake credentials or I/O anywhere in this build.
- **Row-count floors (100 for member transactions, 5 for loan
  originations)** and **stub cardinalities** (member_transactions ~1,200/day,
  deposit_accounts ~450/day, loan_originations ~35/day) — plausible for a
  ~165,000-member credit union per the brief's own public-profile note;
  not real Partners FCU volumes (none are given).
- **No BI tool, no ingestion tool beyond SQL Server, no specific data
  domains beyond generic credit-union nouns** — three genuine "unknown"s
  in the brief, built generic rather than guessed, per house rules.
- **No planted failure, no SSIS/legacy-scheduler modeling** — explicitly
  out of scope per the brief; the graph stays green throughout.
