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
everything else — not black boxes upstream or downstream of it.

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

## Fidelity: graph-first, with dbt running for real

Per the brief, the Fivetran ingestion, Braze activation, and Power BI
reporting layers are graph-first — every body is a trivial no-op
(`GraphFirstAssetsComponent`). **dbt is the exception, by design**: dbt
Core integration depth is explicitly what Iain asked to see, so the dbt
layer runs a real, small dbt Core project (native `dagster-dbt`) against
DuckDB, with real `not_null`/`unique` tests surfaced as Dagster asset
checks automatically.

Real dbt SQL needs real rows to transform and test against. Rather than
build a full synthetic-data-generation apparatus (data-backed fidelity,
which the brief doesn't ask for), `demo_data/bootstrap.py` loads four
static fixture CSVs (`demo_data/fixtures/`) into the DuckDB `raw` schema at
definitions-load time — deterministic, no random generation at runtime.
This stands in for Fivetran having already synced RVU's source systems
into the warehouse; it is **not a Dagster asset or lineage node** (in
production, Fivetran puts the data there, not Dagster). The four
`raw_*` Fivetran-badged assets are the graph-first lineage nodes
representing those same syncs.

Flipping to RVU's real BigQuery is a `profiles.yml` target change (`dbt_project/profiles.yml`
has a `live` output already stubbed), not a code change — same dbt project,
same tests, same asset graph.

## The asset graph

Twelve assets across four groups, one lineage graph:

- **`ingestion`** (4 assets, badged `fivetran`, graph-first) —
  `raw_quote_requests`, `raw_bound_policies`, `raw_panel_insurer_feed`,
  `raw_partner_broker_feed`.
- **`dbt`** (6 assets, badged `dbt`+`bigquery`, real dbt Core) —
  `staging/stg_quote_requests`, `staging/stg_bound_policies`,
  `staging/stg_panel_feed`, `marts/dim_partner`, `marts/fct_quotes_daily`
  (daily partitioned), `marts/fct_bound_policies_daily` (daily
  partitioned, joins `stg_panel_feed` for the insurer's display name).
- **`activation`** (1 asset, badged `braze`, graph-first) —
  `braze_customer_segment_export`, downstream of `fct_quotes_daily`.
- **`reporting`** (1 asset, badged `powerbi`, graph-first) —
  `power_bi_quote_performance_report`, downstream of
  `fct_bound_policies_daily`.

**Note on asset count:** the brief's Build directives name a "~16 assets"
sizing target but then enumerate exactly these twelve by name (4 ingestion
+ 6 dbt + 1 activation + 1 reporting). This build follows the explicit
named list rather than the approximate count, same precedent as
`demos/trafigura` — flagged here so it isn't missed.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| 11 dbt-native `not_null`/`unique` tests | `stg_quote_requests`, `stg_bound_policies`, `stg_panel_feed`, `dim_partner` | Mixed (dbt default) | Missing/duplicate keys — auto-surfaced as Dagster asset checks, zero extra Dagster code. |
| `raw_panel_insurer_feed_completeness` | `raw_panel_insurer_feed` | **Blocking** | Fewer than 7 of Tempcover's panel insurers reporting in a sync. |
| `fct_bound_policies_daily_reconciliation` | `fct_bound_policies_daily` | Warning | The daily fact's summed `policy_count` diverging from raw ingested rows — the aggregation step silently dropping or duplicating. |

**Correction to the brief:** the brief's Asset checks section says the
panel-completeness check "blocks `fct_quotes_daily`" — but panel-insurer
data has no relationship to quote requests in this domain (quotes don't
reference an insurer; only bound policies do). Built as blocking
`raw_panel_insurer_feed` → gating `stg_panel_feed`'s automation instead
(`eager() & all_deps_blocking_checks_passed()`), and `fct_bound_policies_daily`
was given a genuine `ref('stg_panel_feed')` dependency (it now joins the
panel feed for the insurer's display name) so the gating relationship is a
real lineage edge, not just a talking point. Flagged per house rules
rather than building a check with no real dependency behind it.

All three custom-Python checks compute genuine results against the fixture
data in DuckDB (not hardcoded passes) — `validate_e2e.py` asserts they
evaluate to `True`, not just that they ran.

## Freshness, automation, and schedule

- **Freshness policy**: `fct_quotes_daily` and `fct_bound_policies_daily`
  (fail at 24h, warn at 18h) — both named in the brief (Three buckets vs.
  Money shot sections respectively), so both got the policy.
- **Automation conditions**: `AutomationCondition.eager()` on every dbt
  asset; `stg_panel_feed` additionally gates on
  `all_deps_blocking_checks_passed()` (see check correction above).
- **Schedule**: `rvu_morning_marts_schedule` computes the prior day's two
  daily facts at 07:00 Europe/London. **Assumption**: no cutover time is
  named in the brief — this is a plausible UK-morning batch cadence, not a
  stated SLA.
- **Retry policies**: none. Per house rules, a decorative retry on a
  deterministic fixture source invites a question we'd lose; nothing here
  is genuinely flaky.

## Three buckets

1. **Implemented in code**: the twelve-asset graph, dbt's native tests plus
   two custom checks, two freshness policies, eager automation (one
   check-gated), the morning schedule.
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

**5 YAML, 5 Python** (plus boilerplate `defs/__init__.py`) — an even split
by file count, worth being honest about rather than rounding up. But every
one of the 12 assets is defined by a YAML-instantiated component; **zero**
Python files in `defs/` define an asset directly. The five `.py` files are
all non-asset-producing support code — two `@dg.asset_check` functions, two
`@dg.template_var` modules (a Jinja limitation, not a design choice — see
below), and one schedule:

- `defs/ingestion/defs.yaml`, `defs/activation/defs.yaml`,
  `defs/reporting/defs.yaml` — 6 assets, one shared component
  (`GraphFirstAssetsComponent`) instantiated three times.
- `defs/transformation/staging/defs.yaml`,
  `defs/transformation/marts_daily/defs.yaml` — the whole dbt layer (6
  assets), one shared component (`RvuDbtComponent`, a
  `DbtProjectComponent` subclass) instantiated twice (once per partitioning
  need — unpartitioned staging/dims vs. daily-partitioned facts).
- `defs/transformation/staging/template_vars.py`,
  `defs/transformation/marts_daily/template_vars.py` — **justified**:
  Jinja (YAML template expressions) has no `&` operator and `dg`'s
  template scope doesn't expose `AutomationCondition`/`FreshnessPolicy`
  composition directly; exposing the composed Python values as
  `@dg.template_var`s is the documented sanctioned pattern (same as
  `demos/kapitus`, `demos/stellantis-financial-services`).
- `defs/checks/raw_panel_insurer_feed_completeness.py`,
  `defs/checks/fct_bound_policies_daily_reconciliation.py` —
  **justified**: asset-check assertion logic is business logic; no
  registry component covers a declarative completeness/reconciliation
  check (gap first identified in `demos/detroit-dwsd`, 2026-08-28; same
  gap, not re-searched per LEARNINGS.md).
- `defs/automation/morning_batch_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't
  express a specific hour alongside a `partitions_def` (LEARNINGS.md,
  confirmed by reading its source in `demos/detroit-dwsd`), so the
  one-function native `build_schedule_from_partitioned_job` call is used
  directly instead.

`components/graph_first_assets.py`, `components/dbt_project.py`,
`components/partitions.py`, and `demo_data/bootstrap.py` +
`demo_data/warehouse.py` are components/support code, not `defs/` — only
`defs/` is measured for the YAML-first ratio.

## Custom / subclassed components

- **`GraphFirstAssetsComponent`** — reused verbatim, not newly written.
  Same gap and solution first identified in `demos/detroit-dwsd`; full
  search record: `component-feedback/2026-08-28-graph-first-assets.md`.
- **`RvuDbtComponent`** — a thin subclass of the native
  `dagster_dbt.DbtProjectComponent` (rung 1, not a registry gap) that
  overrides `get_asset_spec()` to badge every dbt model `dbt`+`bigquery`
  instead of the manifest-derived `duckdb`, and disambiguates
  `defs_state_config` across the two component instances sharing one
  `project_dir` — same pattern as `demos/kapitus`'s `KapitusDbtComponent`.
  Not a registry-gap component; no feedback file needed.

Registry search performed per the brief's "Community components to search
for" list: `power bi` / `powerbi` and `braze` (zero hits both times —
confirms no registry component exists for either; graph-first `AssetSpec`s
badge the kind instead, per house rules), `fivetran` (registry has
`fivetran_assets`/`fivetran_sync_sensor`/`fivetran_sync_trigger_job`, but
all require a real Fivetran account to discover connectors at
defs-state-write time — same registry gap LEARNINGS.md already records for
S3/EventBridge/ADLS bronze feeds; `GraphFirstAssetsComponent` covers it),
`dbt` (native `dagster-dbt` covers this directly, rung 1).

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
  real numbers exist in the brief; this build's own plausible defaults
  (same numbers used in `demos/trafigura` for the same reason).
- **The panel-completeness check's blocking target**, corrected from
  `fct_quotes_daily` (as literally stated in the brief) to
  `stg_panel_feed`/`fct_bound_policies_daily` — see Asset checks above.
- **Fixture cardinalities** (quote_requests ~1,480 rows across 35 days,
  bound_policies ~400, 7 panel insurers, 85 partners) — plausible for a UK
  temporary-motor-insurance broker per the brief's public-research
  section, not real RVU volumes (none are given).
- **No dbt Semantic Layer, no self-hosted-on-GKE build** — both explicitly
  conversation-only per the brief.
