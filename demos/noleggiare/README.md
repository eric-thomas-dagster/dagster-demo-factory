# Noleggiare (FTH Group) — Dagster demo

## "One BI Team, Two Companies"

## The pitch

Raffaele Vumbaca's five-person BI team already runs Dagster Starter,
unprompted, three months in — a real database migration workflow and an ML
pipeline, self-serve. This isn't a qualification call; it's an SE
architecture review for an engaged trial user deciding how to scale up.

This build shows Dagster as the single orchestration layer across **both**
companies in the FTH Group — Noleggiare (vehicle rental, 10,000+ fleet) and
Tomasi Auto (multi-brand dealer, ~19K vehicles/year) — with one asset graph,
lineage, and data-quality checks sitting between their Postgres-based
sources and Qlik Cloud. It answers two structural questions in the graph
itself: how a two-company BI team stays visibly separate but centrally
governed (asset groups + a `date x company` multi-partition + Dagster+
RBAC), and that swapping Postgres for Snowflake later is a resource
change, not an orchestration rewrite (`future_state_snowflake`).

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required.** This is a graph-first demo — thirteen of the fourteen asset
bodies are no-ops; the one exception (`qlik_cloud_export`) writes a small
simulated-publish manifest to `demo_data/qlik_exports/` (gitignored,
created on first materialize) rather than requiring any credential.

## Fidelity: graph-first

Per the brief, nothing asks for a real number on screen — the explicit ask
is lineage, checks, freshness, ML/ETL coexistence, and warehouse
portability. Every asset body is a no-op (`GraphFirstAssetsComponent`),
except `qlik_cloud_export`, which the brief explicitly directs to carry a
real demo/live network seam (see below) so the "flip one line to go live"
story has one concrete asset behind it.

## The asset graph

Fourteen assets across six groups:

- **`noleggiare_rental_ops`** (3, badged `postgres`) — `rental_bookings_raw`,
  `fleet_vehicles_raw`, `rental_contracts_raw`.
- **`tomasi_dealer_ops`** (3, badged `postgres`) — `vehicle_inventory_raw`,
  `dealer_sales_raw`, `service_orders_raw`.
- **`shared_finance_warehouse`** (5, badged `postgres`) — `dim_vehicle`
  (deps on both companies' vehicle raw feeds — the record the cross-company
  check runs against), `dim_customer` (deps on both companies' customer
  touchpoints), `fact_rental_contract`, `fact_vehicle_sale` (enriched with
  `service_orders_raw` for warranty reserve), and
  `fact_finance_consolidated_daily` — the money-shot asset, partitioned by
  `date x company`.
- **`ml_workflows`** (1, badged `python`) — `fleet_residual_value_forecast`,
  depending on both fact tables, sitting inline in the same lineage as ETL.
- **`bi_publish`** (1, badged `qlik`) — `qlik_cloud_export`, downstream of
  the consolidated fact.
- **`future_state_snowflake`** (1, badged `snowflake`) — 
  `fact_finance_consolidated_daily_snowflake`, the same logical asset on a
  different resource — the literal "swap the warehouse" click-through.

**Note on asset count:** the brief's Build directives name a "(~16-20
assets)" sizing target but then enumerate exactly these fourteen by name,
group by group. This build follows the explicit named list rather than the
parenthetical count — same precedent as `demos/trafigura` and
`demos/rvu-tempcover` — since inventing unnamed assets to hit a number
would be scope not actually specified in the brief.

**Dependency choices not fully specified in the brief:** the brief names
each group's assets but not every downstream edge. To keep every asset
connected (no orphans) and the cross-company check meaningful:
`dim_vehicle` depends on both `fleet_vehicles_raw` and
`vehicle_inventory_raw`; `dim_customer` depends on `rental_bookings_raw`
and `dealer_sales_raw`; `fact_vehicle_sale` additionally depends on
`service_orders_raw` (post-sale service volume feeding warranty
reserve/margin). These are this build's own judgment calls, not sourced.

## Partitions

- **Daily** (`DailyPartitionsDefinition`, UTC, start `2026-08-01`) on the
  six raw ingestion assets plus `fact_rental_contract` and
  `fact_vehicle_sale`. Dimension tables (`dim_vehicle`, `dim_customer`)
  are unpartitioned, per normal Dagster practice for master/reference
  data — the brief's "ingestion and warehouse layers" directive is read as
  covering the fact layer, not the dims.
- **`date x company`** (`MultiPartitionsDefinition`) on
  `fact_finance_consolidated_daily` and its Snowflake-variant twin —
  company is a real second dimension, not a tag, per the brief's explicit
  directive. Verified empirically (see LEARNINGS.md) that a
  multi-partitioned downstream asset with plain `deps:` on
  differently-single-partitioned upstream assets loads and executes
  cleanly with no explicit `PartitionMapping`.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `fact_rental_contract_completeness` | `fact_rental_contract` | **Blocking** | A contract row missing `customer_id`, `vehicle_id`, or `company_id`. |
| `dim_vehicle_cross_company_consistency` | `dim_vehicle` | **Blocking** | A VIN active as both fleet inventory and dealer inventory at once — the double-count that two teams running separate scripts would miss. |
| `fact_finance_consolidated_daily_volume_band` | `fact_finance_consolidated_daily` | Warning | A partition's row count falling outside the expected [200, 2000] band, catching a source going quiet before it starves the Qlik dashboard. |

All three always pass (graph-first, no planted anomaly) and report, in
their metadata, the exact rule they'd enforce against real Postgres data in
production.

## Freshness, automation, and schedule

- **Freshness policy**: `fact_finance_consolidated_daily` (fail at 24h,
  warn at 18h) — the money-shot asset, with an assumed 07:00 CET Finance
  morning-read SLA (no real SLA is confirmed in the AE notes; the brief's
  own credit-sizing sheet gives a ~260-assets/day steady-state estimate but
  no cadence number).
- **Automation conditions**: `AutomationCondition.eager()` on the entire
  `shared_finance_warehouse` layer plus `fleet_residual_value_forecast`,
  per the brief's directive.
- **Schedule**: `noleggiare_morning_ingest_schedule` materializes the six
  raw ingestion assets and the two per-company facts at 04:00 UTC
  (~05:00-06:00 CET/CEST). The cross-company consolidated fact, the
  Snowflake twin, the ML forecast, and the Qlik export are deliberately
  **not** on this schedule — they recompute via eager automation once their
  upstream lands, which is itself part of the demo's automation story.
  `build_schedule_from_partitioned_job` rejects `hour_of_day` combined with
  an explicit `execution_timezone`, so the hour is expressed in the
  partitions_def's own UTC timezone rather than Europe/Rome directly (see
  code comment).
- **Retry policies**: none. Per house rules, a decorative retry on a
  deterministic no-op source invites a question we'd lose; nothing here is
  genuinely flaky.

## The one real network boundary: `qlik_cloud_export`

Per the brief's explicit directive, this asset carries a real
`demo_mode` seam (`QlikCloudExportComponent`,
`src/noleggiare/components/qlik_cloud_export.py`) rather than a no-op body:
`demo_mode: true` (the default, set in `defs/bi_publish/defs.yaml`) writes
a simulated publish manifest to `demo_data/qlik_exports/`; `demo_mode:
false` plus `QLIK_CLOUD_TENANT_URL` / `QLIK_CLOUD_API_KEY` /
`QLIK_CLOUD_APP_ID` calls the real Qlik Cloud reload API. Everything else
in the graph is a no-op — no registry component exists for Qlik Cloud (see
`component-feedback/2026-09-03-qlik-cloud-export.md`), and the brief
doesn't ask for real data anywhere else.

## Three buckets

1. **Implemented in code**: the fourteen-asset graph, three checks, one
   freshness policy, eager automation on the warehouse layer plus the ML
   forecast, the `date x company` multi-partition, per-company asset
   groups, and the real demo/live seam on `qlik_cloud_export`.
2. **Handled by Dagster+, demonstrated not built**: RBAC/permissions
   (the direct answer to the two-company governance question), native
   alerting (Slack/Teams/email/PagerDuty), restart-from-failure, run
   history, lineage visualization, asset health, and the Hybrid-on-AWS
   deployment architecture Raffaele has already asked about — described
   and diagrammed live, not deployed against a real AWS account.
3. **Conversation only, nothing built**: TC8/Forthing (the third FTH Group
   entity, not in the AE notes), and any real production Postgres →
   Snowflake migration runbook — the demo shows orchestration portability,
   not a migration plan.

## `defs/` file count

**6 YAML, 2 Python** (plus a boilerplate empty `defs/__init__.py`):

- `defs/noleggiare_rental_ops/defs.yaml`, `defs/tomasi_dealer_ops/defs.yaml`,
  `defs/shared_finance_warehouse/defs.yaml`, `defs/ml_workflows/defs.yaml`,
  `defs/future_state_snowflake/defs.yaml`, `defs/bi_publish/defs.yaml` —
  every asset, entirely declarative, two shared components
  (`GraphFirstAssetsComponent` five times, `QlikCloudExportComponent`
  once).
- `defs/checks/checks.py` — **justified**: asset-check assertion logic is
  business logic; no registry component covers a declarative
  completeness/cross-company-consistency check (gap first identified in
  the detroit-dwsd build, unchanged, not re-searched). Three checks
  combined into one file since none share code.
- `defs/automation/morning_ingest_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't
  express a specific hour alongside a `partitions_def` (confirmed reading
  its source in the detroit-dwsd build, reused finding), so the
  one-function native `build_schedule_from_partitioned_job` call is used
  directly.

`components/graph_first_assets.py` and `components/qlik_cloud_export.py`
are the two custom components (see below) — components are expected to
hold Python; only `defs/` is measured for the YAML-first ratio.

## Custom components

- **`GraphFirstAssetsComponent`** — reused, not newly written. Same gap
  first identified in `demos/detroit-dwsd`:
  `component-feedback/2026-08-28-graph-first-assets.md`.
- **`QlikCloudExportComponent`** — new. No registry component covers Qlik
  Cloud (only Qlik Compose, a different product under the same vendor):
  `component-feedback/2026-09-03-qlik-cloud-export.md`.

Registry search per the brief's "Community components to search for"
(`postgres`, `qlik`, `snowflake`) confirms: `postgres` and `snowflake`
searches surface only real-connection integration/resource components,
irrelevant to a graph-first build with no I/O layer for them to attach to
(`kinds` badges carry the visual fidelity instead, per house rules,
following the same reasoning as `demos/trafigura`); `qlik` surfaces only
Qlik Compose components (see above).

## Which parts run where

Thirteen of fourteen assets are in-memory no-ops — nothing to persist
between runs, so Dagster+ Serverless's ephemeral storage doesn't affect
them; the same materialize sequence works identically in `dg dev` and the
deployed code location. The one exception, `qlik_cloud_export`, writes a
local file in demo mode — that file doesn't need to survive between runs
(each materialize overwrites it), so it isn't part of any multi-run
recovery story either. `dg dev` is still the natural place to click through
the graph and metadata panels on a shared screen.

## Assumptions

Everything below was inferred, not confirmed in the AE notes — see
`briefs/2026-09-04-noleggiare.md` for full sourcing:

- **The 04:00 UTC / 07:00 CET schedule and SLA.** No real cadence or SLA
  number is confirmed; the brief's own credit-sizing sheet gives only a
  ~260-assets/day steady-state estimate.
- **The 200-2000 row-count band on the volume check.** Invented, plausible
  for the scale implied by a ~260-assets/day estimate, not sourced.
- **Which company is "the higher-volume company"** for the future
  Snowflake migration. Not named in the AE notes; this build treats
  `future_state_snowflake` as company-agnostic (it mirrors the same
  cross-company consolidated fact) rather than asserting either company.
- **Specific rental/dealer domain entities and dependency edges**
  (`dim_vehicle`/`dim_customer` deps, `service_orders_raw` feeding
  `fact_vehicle_sale`) — the brief names each group's assets but not every
  edge; see "Dependency choices" above.
- **No dbt.** Not evidenced for this prospect, despite the Tomasi Auto job
  posting naming Talend/Pentaho/Azure Data Factory as separate tools.
