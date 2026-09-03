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
required.** The two warehouse-badged groups write real (if trivial) stub
rows to a local DuckDB file on first materialize (`src/noleggiare/demo_data/`,
gitignored); `qlik_cloud_export` writes a small simulated-publish manifest
to the same directory. Nothing needs to exist beforehand.

## Fidelity: graph-first content, real I/O (see "The Axis-1 correction" below)

Per the brief, nothing asks for a real number on screen — the explicit ask
is lineage, checks, freshness, ML/ETL coexistence, and warehouse
portability, and every asset body's *content* is stubbed (a handful of
deterministic, seeded placeholder rows — Axis 2). **What changed from the
brief's literal "bodies are pass" wording**: those stub rows are written
and read back through real, native Dagster IO managers
(`dagster-duckdb-pandas`, wired through the real community-registry
`postgres_io_manager` and `snowflake_io_manager` components) rather than
no-op bodies, because CLAUDE.md's Axis-1 rule — "every external system
named in the brief maps to a real component... not affected by fidelity"
— overrides the brief's Fidelity section where the two conflict, and the
house rules say so explicitly. See "The Axis-1 correction" below.

`fleet_residual_value_forecast` (Axis 2, no named external system) stays a
plain no-op body — nothing in the brief asks it to touch a warehouse, and
CLAUDE.md's Axis 2 carve-out covers hand-written custom logic with no
integration surface.

## The Axis-1 correction this build makes

A prior build of this project (PR #16, reverted) routed every
Postgres-badged and Snowflake-badged asset through a home-made
`GraphFirstAssetsComponent` whose body only logged a message — zero real
I/O, even though the brief's own "Native integrations to use" section
names `dagster-postgres` for the current-state layers and
`dagster-snowflake` for the future-state layer, and CLAUDE.md bans exactly
this pattern by construction ("Banned by construction: `GraphFirstAsset`
... A 2026-08 build wrote a `GraphFirstAsset` component and routed
[external systems] through it — while real components existed").

This build fixes that:

- **`postgres_io_manager`** and **`snowflake_io_manager`** — the real,
  unmodified community-registry components (installed via
  `dagster-component add postgres_io_manager` /
  `dagster-component add snowflake_io_manager`, living at
  `src/noleggiare/components/postgres_io_manager/` and
  `.../snowflake_io_manager/`) — are subclassed exactly once each
  (`DemoPostgresIOManagerComponent`, `DemoSnowflakeIOManagerComponent`,
  `src/noleggiare/components/demo_*_io_manager.py`) to add a `demo_mode`
  field. `demo_mode: true` (the default) swaps the IO manager resource for
  `dagster_duckdb_pandas.DuckDBPandasIOManager` — a different real,
  published, native Dagster integration package (`dagster-duckdb-pandas`),
  not a home-made stand-in — because there are no live Postgres/Snowflake
  credentials for this prospect. `demo_mode: false` calls
  `super().build_defs()`, the exact, untouched registry component, so
  pointing either resource at a real database is a one-line change plus
  credentials.
- **`WarehouseTableAssetsComponent`** (`src/noleggiare/components/warehouse_table_assets.py`,
  replacing the banned `GraphFirstAssetsComponent`) builds each declared
  `AssetSpec` a body that writes a handful of deterministic stub rows
  through whichever IO manager the `defs.yaml` file points it at
  (`io_manager_key: postgres_io_manager` or `snowflake_io_manager`). Asset
  keys, deps, partitions, metadata, kinds, and checks all still come from
  the YAML spec, unchanged from the prior design — only the body performs
  genuine I/O now.
- **Registry search, run this build** (per the brief's own suggested
  terms, `--json`, 2026-09-03):
  - `dagster-component search "postgres" --json` → `postgres_io_manager`
    (io_manager, "Register a PostgreSQL IO manager so assets are
    automatically stored in and loaded from Postgres tables") is the clear
    fit — used as-is (rung 2), subclassed only for the demo-mode seam.
  - `dagster-component search "snowflake" --json` → `snowflake_io_manager`
    (io_manager, "Register a SnowflakePandasIOManager...") — same
    treatment, wrapping the real native `dagster-snowflake-pandas` package.
  - No workspace-style component that *generates* the asset specs
    themselves exists for either (both are pure IO-manager resources you
    attach to your own asset definitions), so `WarehouseTableAssetsComponent`
    remains the asset-body factory — same registry gap as the no-op
    predecessor it replaces (`component-feedback/2026-08-28-graph-first-assets.md`),
    now performing real I/O instead of logging.
- **`validate_e2e.py`** now asserts, beyond "every partition materialized,"
  that both demo-mode DuckDB files actually contain tables with rows in
  them — the concrete proof this correction holds.

**System → component-ID mapping** (every system the brief names):

| System (brief) | Component ID | Package | Real component? |
|---|---|---|---|
| PostgreSQL | `postgres_io_manager` | community registry, wraps SQLAlchemy + psycopg2 | Yes — subclassed for demo-mode seam only |
| Snowflake | `snowflake_io_manager` | community registry, wraps `dagster-snowflake-pandas`'s `SnowflakePandasIOManager` | Yes — subclassed for demo-mode seam only |
| Qlik Cloud | *(no registry component — see below)* | custom `QlikCloudExportComponent` | Rung-4 custom, properly escalated (see below) |

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
  Axis 2, no-op body, no IO manager — see "Fidelity" above.
- **`bi_publish`** (1, badged `qlik`) — `qlik_cloud_export`, downstream of
  the consolidated fact.
- **`future_state_snowflake`** (1, badged `snowflake`) —
  `fact_finance_consolidated_daily_snowflake`, the same logical asset on a
  different, genuinely different resource — the literal "swap the
  warehouse" click-through, with real I/O landing in a *separate* DuckDB
  file from the Postgres-badged assets.

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
  cleanly with no explicit `PartitionMapping`. The real IO manager's
  partition-scoped writes need a `partition_expr` metadata value naming the
  column each partition dimension lives in; `WarehouseTableAssetsComponent`
  sets this automatically and writes real `date`/`company` columns into
  the stub rows so the DELETE+INSERT the IO manager runs on rematerialize
  actually scopes correctly.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `fact_rental_contract_completeness` | `fact_rental_contract` | **Blocking** | A contract row missing `customer_id`, `vehicle_id`, or `company_id`. |
| `dim_vehicle_cross_company_consistency` | `dim_vehicle` | **Blocking** | A VIN active as both fleet inventory and dealer inventory at once — the double-count that two teams running separate scripts would miss. |
| `fact_finance_consolidated_daily_volume_band` | `fact_finance_consolidated_daily` | Warning | A partition's row count falling outside the expected [200, 2000] band, catching a source going quiet before it starves the Qlik dashboard. |

All three always pass (per the brief's graph-first Fidelity directive for
check *logic* — unaffected by the Axis-1 I/O fix above) and report, in
their metadata, the exact rule they'd enforce against real Postgres data
in production.

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
  deterministic stub source invites a question we'd lose; nothing here is
  genuinely flaky.

## The real network/write boundaries

Three assets (well, three IO-manager-backed groups) carry a genuine
demo/live seam, per `templates/demo_mode_pattern.py`:

- **`postgres_io_manager`** (`DemoPostgresIOManagerComponent`) — backs
  `noleggiare_rental_ops`, `tomasi_dealer_ops`, `shared_finance_warehouse`.
  `demo_mode: true` (default, set in `defs/resources/postgres/defs.yaml`)
  writes to `src/noleggiare/demo_data/noleggiare_postgres.duckdb`.
  `demo_mode: false` plus real `host` / `database` / `user` /
  `password_env_var` runs the exact, unmodified registry
  `PostgresIOManagerComponent` against a live PostgreSQL instance.
- **`snowflake_io_manager`** (`DemoSnowflakeIOManagerComponent`) — backs
  `future_state_snowflake`. Same pattern, writing to
  `src/noleggiare/demo_data/noleggiare_snowflake.duckdb` in demo mode;
  `demo_mode: false` plus real `account` / `user` / `password_env_var` /
  `database` / `warehouse` calls the real, unmodified
  `SnowflakeIOManagerComponent` wrapping `dagster-snowflake-pandas`.
- **`qlik_cloud_export`** (`QlikCloudExportComponent`,
  `src/noleggiare/components/qlik_cloud_export.py`) — `demo_mode: true`
  (default, set in `defs/bi_publish/defs.yaml`) writes a simulated publish
  manifest to `src/noleggiare/demo_data/qlik_exports/`; `demo_mode: false`
  plus `QLIK_CLOUD_TENANT_URL` / `QLIK_CLOUD_API_KEY` / `QLIK_CLOUD_APP_ID`
  calls the real Qlik Cloud reload API. No registry component covers Qlik
  Cloud (see `component-feedback/2026-09-03-qlik-cloud-export.md`,
  reused unchanged from the prior build — it was already a properly
  escalated rung-4 component, not the defect this rebuild fixes).

## Three buckets

1. **Implemented in code**: the fourteen-asset graph, three checks, one
   freshness policy, eager automation on the warehouse layer plus the ML
   forecast, the `date x company` multi-partition, per-company asset
   groups, and genuine I/O through the real (subclassed-for-demo-mode)
   Postgres/Snowflake IO manager components plus the real demo/live seam
   on `qlik_cloud_export`.
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

**7 YAML, 2 Python** (plus a boilerplate empty `defs/__init__.py`):

- `defs/noleggiare_rental_ops/defs.yaml`, `defs/tomasi_dealer_ops/defs.yaml`,
  `defs/shared_finance_warehouse/defs.yaml`, `defs/future_state_snowflake/defs.yaml`,
  `defs/bi_publish/defs.yaml`, `defs/resources/postgres/defs.yaml`,
  `defs/resources/snowflake/defs.yaml` — every asset and every IO manager
  registration, entirely declarative, three shared components
  (`WarehouseTableAssetsComponent` four times, `DemoPostgresIOManagerComponent`
  once, `DemoSnowflakeIOManagerComponent` once, `QlikCloudExportComponent`
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
- `defs/ml_workflows/ml_workflows.py` — **justified**: the one hand-written
  Axis-2 custom-logic asset with no named external system, per CLAUDE.md's
  Axis-2 carve-out. Routing it through `WarehouseTableAssetsComponent`
  would badge it postgres/snowflake, which it doesn't claim to be.

`components/demo_postgres_io_manager.py`, `components/demo_snowflake_io_manager.py`,
`components/warehouse_table_assets.py`, and `components/qlik_cloud_export.py`
are this build's own Python (plus the two unmodified registry-installed
packages, `components/postgres_io_manager/` and
`components/snowflake_io_manager/`) — components are expected to hold
Python; only `defs/` is measured for the YAML-first ratio.

## Custom / subclassed components

- **`DemoPostgresIOManagerComponent`**, **`DemoSnowflakeIOManagerComponent`**
  — rung-3 subclasses of the real registry `postgres_io_manager` /
  `snowflake_io_manager` components. See "The Axis-1 correction" above.
- **`WarehouseTableAssetsComponent`** — reused pattern (formerly
  `GraphFirstAssetsComponent`, banned by CLAUDE.md by name), rewritten to
  perform genuine I/O. Same registry gap as
  `component-feedback/2026-08-28-graph-first-assets.md` (declaring a list
  of assets from YAML with a shared body is a generic authoring
  convenience, not an integration domain — it never appears in `kinds`).
- **`QlikCloudExportComponent`** — rung-4 custom, unchanged from the prior
  build. No registry component covers Qlik Cloud (only Qlik Compose, a
  different product under the same vendor):
  `component-feedback/2026-09-03-qlik-cloud-export.md`.

## Which parts run where

Every warehouse-backed asset writes to a local DuckDB file in demo mode —
`src/noleggiare/demo_data/noleggiare_postgres.duckdb` and
`.../noleggiare_snowflake.duckdb`. **Dagster+ Serverless storage is
ephemeral** (fresh container per run), so these files do not persist
between separate runs in the cloud deployment. That's fine here: nothing
in this demo depends on data written in one run being read in a later
run — every asset's `deps:` are lineage-only (no asset reads another's
output value), so each run is self-contained regardless of what happened
in a prior run. **Give the live demo locally with `dg dev`**, where you
have control on a shared screen; treat the Dagster+ deployment as proof
the project loads and the graph renders, not as the place to click through
a multi-run persistence story.

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
- **Stub row content and count (5 rows/partition).** The brief's Fidelity
  section literally says "bodies are pass"; this build deviates per
  CLAUDE.md's explicit precedence rule (Axis 1 always wins over a
  brief's fidelity wording) so the Postgres/Snowflake badges reflect real
  I/O. Row content itself stays trivial and deterministic — Axis 2 is
  unaffected.
