# RVU (Tempcover) — Dagster demo

## "From Ran to Right"

Azure Data Factory can tell Lisa's team a job ran. This shows whether the
data was right.

## The pitch

Iain is one month into leading a from-scratch data-platform rebuild that
isn't optional (legacy platform deprecation is forcing it). BigQuery and
dbt Core are already decided; orchestration is the one open decision, and
Iain is already advocating internally for Dagster over Cloud Composer,
unprompted. This demo has one job: turn that instinct into a concrete
technical argument he can hand to Tom (VP Engineering, not in the room,
waiting on a cost number).

It shows real per-asset observability replacing RVU's hand-built warehouse
logging, dbt Core surfacing as first-class assets and checks with no extra
plumbing, and Fivetran/Braze/Power BI sitting in the same lineage graph as
everything else — not black boxes upstream or downstream of it. **Every
external system in the graph runs through the real, corresponding Dagster
integration** (`dagster-fivetran`, `dagster-powerbi`, native `dagster-dbt`)
with the network boundary mocked, not a home-made stand-in — see
"Integration mapping" below for why this build exists.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required.** `RVU_DEMO_DUCKDB_PATH` defaults to a project-relative DuckDB
file, created (and its `raw` schema fixture tables reloaded) automatically
the moment Dagster's definitions load — see "Fidelity" below.

## Integration mapping — every system, its real component

This rebuild exists to fix a defect: a prior build routed Fivetran, ADF,
and Power BI through a home-made `GraphFirstAssetsComponent` (banned by
construction per `CLAUDE.md`) instead of the real integrations. Every
external system named in the brief now maps to a real Dagster component,
subclassed for the demo-mode I/O seam (`templates/demo_mode_pattern.py`) —
never substituted:

| System | Component | Rung | Escalation reasoning |
|---|---|---|---|
| **Fivetran** (ingestion) | `dagster_fivetran.FivetranAccountComponent`, subclassed as `RvuFivetranComponent` | 1 (native), subclassed per rung 3 | Native package, already follows the workspace-component convention (`get_asset_spec`, `polling_sensor`, `StateBackedComponent`). Subclass mocks `write_state_to_path` (discovery) and `execute` (sync) — see `components/fivetran.py`. |
| **dbt Core** (transformation) | `dagster_dbt.DbtProjectComponent`, subclassed as `RvuDbtComponent` | 1 (native), subclassed for kind-badge only | Runs for real in both demo and live mode — no I/O seam needed. Subclass only overrides `get_asset_spec` to badge `dbt`+`bigquery` instead of the manifest-derived `duckdb`. |
| **Power BI** (reporting) | `dagster_powerbi.PowerBIWorkspaceComponent`, subclassed as `RvuPowerBIComponent` | 1 (native), subclassed per rung 3 | `dagster-component search "power bi" --json` returned zero registry hits — native `dagster-powerbi` is the right rung. Subclass mocks `write_state_to_path` and `build_semantic_model_refresh_asset_definition` — see `components/powerbi.py` for why the report is modeled as its refreshable semantic model. |
| **Braze** (activation) | none found — plain `@dg.asset` | 4 (custom, no component) | Zero hits across three distinct registry searches, no native package. See `component-feedback/2026-09-03-braze-export.md` for the full search record and suggested registry addition. Per the brief's own fallback, a one-object plain asset (not a full custom component) is the right size here — no enumerate/execute/observe triad to generalize. |
| **BigQuery** (warehouse) | `kinds={"bigquery"}` badge on every dbt/ingestion asset | n/a | Engine is DuckDB in demo mode (matching house rules on zero-setup); badge matches RVU's decided target warehouse. Flipping to real BigQuery is a `profiles.yml` target change. |

## Fidelity: graph-first for integrations, dbt runs for real

Per the brief, the Fivetran ingestion, Braze activation, and Power BI
reporting layers are graph-first — every demo-mode execution body is a
thin simulation of the real network call (see the components above), not
a data-generation exercise. **dbt is the exception, by design**: dbt Core
integration depth is explicitly what Iain asked to see, so the dbt layer
runs a real, small dbt Core project (native `dagster-dbt`) against DuckDB,
with real `not_null`/`unique` tests surfaced as Dagster asset checks
automatically.

Real dbt SQL needs real rows to transform and test against. Rather than
build a full synthetic-data-generation apparatus (data-backed fidelity,
which the brief doesn't ask for), `demo_data/bootstrap.py` loads four
static fixture CSVs (`demo_data/fixtures/`) into the DuckDB `raw` schema at
definitions-load time — deterministic, no random generation at runtime.
This stands in for Fivetran having already synced RVU's source systems
into the warehouse; it is **not a Dagster asset or lineage node** (in
production, Fivetran puts the data there, not Dagster — CLAUDE.md: "Mock
source state lives outside Dagster"). The four Fivetran-badged assets'
demo-mode execution bodies read row counts back from this same warehouse,
the same way a real sync would report on rows it just landed.

Flipping to RVU's real BigQuery is a `profiles.yml` target change
(`dbt_project/profiles.yml` has a `live` output already stubbed), not a
code change — same dbt project, same tests, same asset graph. Flipping
Fivetran or Power BI to live mode is a `demo_mode: false` + real
credentials change in the respective `defs.yaml` — same asset keys, specs,
partitions, and YAML schema either way (`templates/demo_mode_pattern.py`).

## The asset graph

Twelve assets across four groups, one lineage graph:

- **`ingestion`** (4 assets, badged `fivetran`, real `FivetranAccountComponent` subclass) —
  `raw_quote_requests`, `raw_bound_policies`, `raw_panel_insurer_feed`,
  `raw_partner_broker_feed`. One `RvuFivetranComponent` instance with an
  explicit `connectors:` mapping table in `defs/ingestion/defs.yaml` — adding
  RVU's next Fivetran-synced source is one more entry, not a new component
  instance.
- **`dbt`** (6 assets, badged `dbt`+`bigquery`, real dbt Core) —
  `staging/stg_quote_requests`, `staging/stg_bound_policies`,
  `staging/stg_panel_feed`, `marts/dim_partner`, `marts/fct_quotes_daily`
  (daily partitioned), `marts/fct_bound_policies_daily` (daily
  partitioned, joins `stg_panel_feed` for the insurer's display name).
- **`activation`** (1 asset, badged `braze`, plain demo-mode-mocked `@asset`) —
  `braze_customer_segment_export`, downstream of `fct_quotes_daily`.
- **`reporting`** (1 asset, badged `powerbi`, real `PowerBIWorkspaceComponent` subclass) —
  `power_bi_quote_performance_report`, downstream of
  `fct_bound_policies_daily`.

**Note on asset count:** the brief's Build directives name a "~16 assets"
sizing target but then enumerate exactly these twelve by name (4 ingestion
+ 6 dbt + 1 activation + 1 reporting). This build follows the explicit
named list rather than the approximate count.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| 13 dbt-native `not_null`/`unique` tests | `stg_quote_requests`, `stg_bound_policies`, `stg_panel_feed`, `dim_partner`, `fct_quotes_daily`, `fct_bound_policies_daily` | Mixed (dbt default) | Missing/duplicate keys — auto-surfaced as Dagster asset checks, zero extra Dagster code. |
| `raw_panel_insurer_feed_completeness` | `raw_panel_insurer_feed` | **Blocking** | Fewer than 7 of Tempcover's panel insurers reporting in a sync. |
| `fct_bound_policies_daily_reconciliation` | `fct_bound_policies_daily` | Warning | The daily fact's summed `policy_count` diverging from raw ingested rows — the aggregation step silently dropping or duplicating. |

**Correction to the brief:** the brief's Asset checks section says the
panel-completeness check "blocks `fct_quotes_daily`" — but panel-insurer
data has no relationship to quote requests in this domain (quotes don't
reference an insurer; only bound policies do). Built as blocking
`raw_panel_insurer_feed` → gating `stg_panel_feed`'s automation instead
(`eager() & all_deps_blocking_checks_passed()`), and `fct_bound_policies_daily`
was given a genuine `ref('stg_panel_feed')` dependency (it joins the panel
feed for the insurer's display name) so the gating relationship is a real
lineage edge, not just a talking point.

All checks compute genuine results against the fixture data in DuckDB (not
hardcoded passes) — `validate_e2e.py` asserts they evaluate to `True`, not
just that they ran.

## Freshness, automation, and schedule

- **Freshness policy**: `fct_quotes_daily` and `fct_bound_policies_daily`
  (fail at 24h, warn at 18h) — both named in the brief.
- **Automation conditions**: `AutomationCondition.eager()` on every dbt
  asset; `stg_panel_feed` additionally gates on
  `all_deps_blocking_checks_passed()` (see check correction above).
- **Schedule**: `rvu_morning_marts_schedule` computes the prior day's two
  daily facts at 07:00 Europe/London. **Assumption**: no cutover time is
  named in the brief — this is a plausible UK-morning batch cadence, not a
  stated SLA.
- **Observation sensor**: `fivetran_rvu_demo_account__sync_status_sensor`
  (`polling_sensor: true` on `RvuFivetranComponent`, default is off)
  detects Fivetran syncs Dagster didn't trigger — Fivetran's own auto
  schedule firing outside of Dagster — and emits observations. `dagster-
  powerbi`'s `PowerBIWorkspaceComponent` has no equivalent polling-sensor
  field; not added here since the brief doesn't call for observing
  externally-triggered Power BI refreshes specifically.
- **Retry policies**: none. Per house rules, a decorative retry on a
  deterministic fixture source invites a question we'd lose; nothing here
  is genuinely flaky.

## Three buckets

1. **Implemented in code**: the twelve-asset graph (Fivetran ingestion via
   the real `dagster-fivetran` integration, real dbt Core with native tests
   plus two custom checks, Power BI reporting via the real `dagster-powerbi`
   integration, Braze activation as a plain demo-mode asset), two freshness
   policies, eager automation (one check-gated), the morning schedule, the
   Fivetran observation sensor.
2. **Handled by Dagster+, demonstrated not built**: branch deployments
   against BigQuery (Iain's own named mechanical question — see
   DEMO_SCRIPT.md), restart-from-failure and run history (the direct
   answer to "any failure requires rerunning the entire job"), native
   alerting (Slack/email/PagerDuty), RBAC and viewer licenses, an
   insights/observability dashboard, an admin log view.
3. **Conversation only, nothing built**: dbt Semantic Layer / structured
   LLM data access (dbt's own roadmap); self-hosted-on-GKE as a Dagster+
   Hybrid alternative — described, not built.

## `defs/` file count

**5 YAML, 6 Python** (plus boilerplate `defs/__init__.py`) — every asset
except one (`braze_customer_segment_export`) is defined by a
YAML-instantiated component; only that one Python file in `defs/` defines
an asset directly, and it's justified (no component exists for its
system — see "Integration mapping" above):

- `defs/ingestion/defs.yaml` — 4 assets, `RvuFivetranComponent` (real
  `dagster-fivetran` subclass) with an explicit `connectors:` mapping table.
- `defs/reporting/defs.yaml` — 1 asset, `RvuPowerBIComponent` (real
  `dagster-powerbi` subclass).
- `defs/transformation/staging/defs.yaml`,
  `defs/transformation/marts_daily/defs.yaml` — the whole dbt layer (6
  assets), `RvuDbtComponent` (real `dagster-dbt` subclass) instantiated
  twice (once per partitioning need — unpartitioned staging/dims vs.
  daily-partitioned facts).
- `defs/activation/braze_customer_segment_export.py` — **justified**: no
  registry or native component exists for Braze (three distinct searches,
  zero hits — `component-feedback/2026-09-03-braze-export.md`); a
  one-object plain `@asset` is the right size per the brief's own fallback.
- `defs/transformation/staging/template_vars.py`,
  `defs/transformation/marts_daily/template_vars.py` — **justified**:
  Jinja (YAML template expressions) has no `&` operator and `dg`'s
  template scope doesn't expose `AutomationCondition`/`FreshnessPolicy`
  composition directly; exposing the composed Python values as
  `@dg.template_var`s is the documented sanctioned pattern.
- `defs/checks/raw_panel_insurer_feed_completeness.py`,
  `defs/checks/fct_bound_policies_daily_reconciliation.py` —
  **justified**: asset-check assertion logic is business logic; no
  registry component covers a declarative completeness/reconciliation
  check against an arbitrary DuckDB query.
- `defs/automation/morning_batch_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't
  express a specific hour alongside a `partitions_def`, so the
  one-function native `build_schedule_from_partitioned_job` call is used
  directly instead.

`components/fivetran.py`, `components/powerbi.py`, `components/dbt_project.py`,
`components/partitions.py`, and `demo_data/bootstrap.py` +
`demo_data/warehouse.py` are components/support code, not `defs/` — only
`defs/` is measured for the YAML-first ratio.

## Registry search record

Per the brief's "Community components to search for" list and CLAUDE.md's
escalation ladder (at least three searches before writing custom code):

```
dagster-component search "fivetran" --json
```
Returns `fivetran_assets`, `fivetran_sync_sensor`,
`fivetran_sync_trigger_job` (community registry wrappers) — but the
**native** `dagster_fivetran.FivetranAccountComponent` package already
exists and follows the workspace-component convention more completely
(StateBackedComponent, `get_asset_spec` hook, `polling_sensor`), so it was
used directly (rung 1) rather than the registry components (rung 2).

```
dagster-component search "power bi" --json
```
Zero hits for a Power BI-specific component. **Native** `dagster_powerbi.
PowerBIWorkspaceComponent` covers it (rung 1).

```
dagster-component search "braze" --json
dagster-component search "customer engagement marketing activation" --json
dagster-component search "braze customer segment export" --json
```
Zero hits, all three — see `component-feedback/2026-09-03-braze-export.md`.

## Which parts run where

The dbt layer's DuckDB file and fixture data are local — they exist for the
lifetime of one process (or one Dagster+ Serverless container). **Give the
interactive demo locally with `dg dev`** — multi-materialization sequences
(e.g. re-running a partition after changing a fixture) only persist there.
Treat the deployed Dagster+ code location as proof the project is real and
loads correctly, not as a place to run a multi-run recovery sequence.

## Assumptions

- **The 07:00 Europe/London schedule hour** — no cutover time is named in
  the brief; a plausible UK-morning batch cadence, not a sourced SLA.
- **Freshness thresholds (24h fail / 18h warn)** on both daily facts — no
  real numbers exist in the brief; this build's own plausible defaults.
- **The panel-completeness check's blocking target**, corrected from
  `fct_quotes_daily` (as literally stated in the brief) to
  `stg_panel_feed`/`fct_bound_policies_daily` — see Asset checks above.
- **Fixture cardinalities** (quote_requests ~1,480 rows across 35 days,
  bound_policies ~400, 7 panel insurers, 85 partners) — plausible for a UK
  temporary-motor-insurance broker per the brief's public-research
  section, not real RVU volumes (none are given).
- **`power_bi_quote_performance_report` modeled as its backing semantic
  model** — Power BI reports/dashboards are read-only views; the real
  triggerable action is a dataset refresh, so that's what materializing
  this asset does (see `components/powerbi.py`).
- **No dbt Semantic Layer, no self-hosted-on-GKE build** — both explicitly
  conversation-only per the brief.
