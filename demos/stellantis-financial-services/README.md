# The 700th Package -- Stellantis Financial Services demo

A Dagster demo built for a first tailored technical demo with Nick Gogos
(Enterprise Data Management lead) and Chris Rodriguez (enterprise data
platform lead) at Stellantis Financial Services (SFS). SFS is migrating
~700 homegrown-orchestrated SSIS packages onto Microsoft Fabric while the
data team triples in size, and is evaluating Dagster as the orchestration
layer on top of -- or replacing -- their homegrown scheduler.

## The thesis

SFS's biggest complaint about the system they built themselves: failure
recovery is manual, replay is weak, and visibility is fragmented -- while
their 2026 capital strategy calls for up to eight auto/lease ABS
securitizations, which makes loan-tape accuracy an investor- and
rating-agency-facing concern, not just an internal one. They are **not**
asking Dagster to rebuild their bronze/silver/gold transformation logic --
that already exists inside the SSIS packages and stored procedures being
migrated into Fabric pipelines. The migration is nowhere near finished, so
this demo has to prove Dagster can sit across **both halves** of it at once:
the SSIS packages still running exactly as they do today, under SFS's own
scheduler, and the pipelines already cut over to Fabric -- in one lineage
graph, with data actually flowing from the legacy side into the migrated
side, not two disconnected demos glued together with a metadata label.

## Running it

```bash
uv sync
dg dev
```

Open http://localhost:3000. **No environment variables or credentials are
required** -- the component defaults to demo mode, reading/writing a local
DuckDB file created on first run at
`src/stellantis_financial_services/demo_data/demo.duckdb`, standing in for
both the Fabric lakehouse tables the migrated pipelines write to and the
legacy system's own shared storage.

Everything runs via `dg`, never the legacy `dagster` CLI:

```bash
dg check defs                                                        # validate definitions
dg list defs                                                         # list every asset
dg launch --assets 'raw_loan_originations' --partition '2026-08-20'  # materialize one Fabric-migrated daily partition
python validate_e2e.py                                               # full end-to-end harness (see below)
```

`raw_dealer_floorplan_feed` and `raw_credit_bureau_pull` are **not**
launchable via `dg launch` -- there is no Dagster compute behind them (see
below). To see them "arrive," open Sensors → `legacy_scheduler_observer` in
the UI and click "Test sensor," or just wait for its 30s poll interval.

## The asset graph -- migration-both-states, not migration-destination-only

17 assets: **2 genuinely legacy, 15 Fabric-migrated.** This is the point of
the demo, not a footnote -- see `CLAUDE.md`'s "Migration prospects: show
current AND future state."

**Legacy (SSIS, under SFS's own homegrown scheduler -- not migrated).**
Plain `AssetSpec`s with zero Dagster-owned compute
(`defs/legacy_assets/legacy_assets.py`). Dagster never triggers these; a
dedicated sensor, `legacy_scheduler_observer`, polls SFS's own (mocked)
scheduler run log and reports completions as `AssetObservation` events. Their
materialization history in the UI is **entirely observations, never a
Dagster-triggered run**:

- `raw_dealer_floorplan_feed` -- `date` x `dealer_group` (4 regions), the one
  asset with a genuine second partition dimension, since floorplan financing
  arrives per-region. A plausible last-to-migrate candidate: third-party
  dealer integration.
- `raw_credit_bureau_pull` -- daily. Same reasoning: external bureau
  integration.

**Fabric-migrated (15 assets)**, one `DemoFabricWorkspaceComponent` instance
(`defs/fabric_pipelines/defs.yaml`) covering all of them via an explicit
`assets_by_item_name` mapping table -- adding SFS's next migrated package is
one more entry in that table, not a new component instance or Python file
(the same shape as `assets_by_task_key` on the community registry's
Databricks workspace component). Every asset is a **trigger-and-observe
wrapper around a Fabric Data Pipeline item** -- Dagster triggers, tracks, and
gates them; it does not reimplement their transformation logic.

- Bronze (`bronze_ingestion`, `date`-partitioned): `raw_loan_originations`,
  `raw_lease_originations`, `raw_payment_transactions`.
- Silver (`silver_transformation`): `stg_loan_originations`,
  `stg_lease_originations`, `stg_payment_transactions`,
  `stg_delinquency_events`, and the two assets **where the lineage actually
  crosses the legacy/Fabric boundary**:
  - `dim_dealer` -- fully Fabric-migrated, but its only input
    (`raw_dealer_floorplan_feed`) is still legacy. Rolls up all four
    `dealer_group` partitions for one date via a
    `MultiToSingleDimensionPartitionMapping`.
  - `dim_borrower` -- fully Fabric-migrated, one of its inputs
    (`raw_credit_bureau_pull`) is still legacy.
- Gold (`gold_marts`): `fact_loan_portfolio`, `fact_delinquency_snapshot`
  (freshness-policed), `abs_pool_eligibility` (the money-shot terminal asset
  -- freshness-policed + blocking-checked), `gl_reconciliation_summary`, and
  `customer_360`.
- Reporting (`reporting`): `powerbi_portfolio_dashboard_refresh` -- a single
  Fabric-native Power BI refresh-trigger asset (see Three buckets below for
  why it isn't a real embedded report).

## Why the legacy assets have no Dagster compute

Assets are idempotent -- rematerializing re-reads the source, it doesn't
"heal" anything (`CLAUDE.md`). For `raw_dealer_floorplan_feed` and
`raw_credit_bureau_pull`, the source is SFS's own scheduler, not Fabric, and
Dagster genuinely never calls anything to produce them -- so giving them a
Dagster compute function the way the Fabric-side bronze assets have one would
be dishonest about who's actually running SFS's estate today.

Instead, `demo_data/legacy_scheduler.py`'s `ensure_legacy_data_landed` mocks
the legacy system's own shared storage (idempotent, deterministic, seeded)
-- called by `legacy_scheduler_observer` to report what already landed, and
by `dim_dealer` / `dim_borrower` when they read across the boundary, the same
way they'd read a shared lakehouse table in production regardless of which
system wrote to it. Nothing under either legacy asset's own key ever
executes as a Dagster computation.

## Fidelity: data-backed

This demo needs real values on screen -- the reconciliation and completeness
checks compute their pass/fail from actual synthesized rows, the
dealer-floorplan lateness check compares a real arrival timestamp against an
SLA, and the ABS-pool and delinquency numbers look like a real loan
portfolio when asked "how many contracts is that." DuckDB stands in for both
the Fabric lakehouse and the legacy system's own storage; generation is
deterministic and seeded (`demo_data/generators.py`,
`demo_data/legacy_scheduler.py`), so row counts and dollar figures never
drift between runs.

## Asset checks (3)

| Check | Asset | Severity | Maps to |
|---|---|---|---|
| `raw_loan_originations_completeness` | `raw_loan_originations` | Blocking | "failure recovery is manual, replay is weak" |
| `abs_pool_eligibility_reconciliation` | `abs_pool_eligibility` | Blocking | 2026 ABS securitization calendar / investor loan-tape accuracy |
| `raw_dealer_floorplan_feed_lateness` | `raw_dealer_floorplan_feed` | Warning | "asset-level expected timing / lateness visibility" |

All three compute real conditions from the synthetic data and pass in this
demo. The first two run as part of a normal Fabric-triggered materialization.
The third is declared the normal way (`@dg.asset_check`, visible and
described on the asset's Checks tab), but since `raw_dealer_floorplan_feed`
is never Dagster-materialized, there's no run for it to ride alongside --
`legacy_scheduler_observer` evaluates it directly and reports the result as
an `AssetCheckEvaluation` in the same tick as its `AssetObservation`. (A
standalone checks-only job was tried first and dropped: building a job from
`AssetSelection.checks_for_assets(...)` over a non-executable asset can't
infer a `PartitionsDefinition` in dagster==1.13.19 -- there's no executable
node in the job for Dagster to infer one from. See the check module's
docstring.) A fourth check (bronze-to-silver row-count reconciliation on
payment transactions) was in the brief as "optional if time allows" and was
skipped to keep scope disciplined.

## Automation

- **Freshness policies**: `fact_delinquency_snapshot` and
  `abs_pool_eligibility` (6h fail / 3h warn) -- the direct answer to "how
  would we know something broke," paged against the ABS pool calendar,
  regardless of whether the upstream data came from Fabric or from the
  system SFS is leaving.
- **Declarative automation**: `AutomationCondition.eager()` on every
  Fabric-side silver/gold/reporting asset (12 of 15) -- once bronze lands,
  everything downstream recomputes itself, including `dim_dealer` /
  `dim_borrower` when a corrected legacy partition is observed. The 3
  Fabric-side bronze feeds are schedule-triggered instead (see below), since
  there's no upstream Dagster asset for them to react to -- a vendor file
  landing is the real-world trigger. The 2 legacy assets have no automation
  condition of their own -- SFS's scheduler decides when they run, not
  Dagster.
- **Schedule**: `bronze_ingestion_daily_6am` fires the three Fabric-migrated,
  single `date`-partitioned bronze feeds at 6am -- the same overnight-batch
  cutoff the floorplan lateness check holds every region to.
- **Fabric-side polling sensor**: `fabric_pipelines_external_run_observer`
  (opt-in via `generate_sensor: true`) detects **already-migrated** Fabric
  pipeline runs Dagster did not trigger -- an operator running one by hand, or
  SFS's scheduler calling the Fabric API directly mid-cutover -- and emits
  `AssetObservation` events. A separate, smaller coexistence story from the
  legacy sensor below. See "Component notes" for why this had to be built
  into the subclass rather than just configured.
- **Legacy observation sensor**: `legacy_scheduler_observer`
  (`defs/legacy_assets/legacy_assets.py`) polls SFS's own (mocked) scheduler
  run log for the two still-legacy feeds and reports completions -- never
  triggers anything. This is the boundary-crossing sensor, distinct from the
  Fabric-side one above.
- **No `RetryPolicy`** on any asset. Every Fabric-migrated source here is a
  Fabric pipeline triggered through the same Fabric REST API, not a
  genuinely flaky dealer-submitted feed with its own failure modes -- a
  decorative retry would invite a question ("what specifically is flaky
  here?") we'd lose. Skipped rather than padded, per `CLAUDE.md`.

## `defs/` file count

10 YAML files (the Fabric component instance + registry-installed component
metadata) vs. 7 Python files in `defs/` (3 asset checks + 1 schedule module +
1 legacy-assets module + 2 `__init__.py`). Every `.py` file's justification:

- `defs/checks/*.py` (3 files) -- asset checks are core-Dagster
  `@dg.asset_check`-decorated Python functions; there is no YAML form for a
  custom SQL assertion, and no registry check component fit this domain
  (see Component notes -- three distinct registry searches came up empty for
  a SQL/DuckDB-native check).
- `defs/automation/bronze_ingestion_schedule.py` -- `define_asset_job` +
  `build_schedule_from_partitioned_job` are core-Dagster Python APIs with no
  YAML component wrapping them in this project.
- `defs/legacy_assets/legacy_assets.py` -- a plain `AssetSpec` declaring an
  external asset, plus a hand-written `@dg.sensor`, is core Dagster
  capability per `CLAUDE.md`'s "Orchestrating existing workloads" section,
  not a rung-4 custom component -- SFS's own scheduler is bespoke and
  in-house, so there's no vendor to search the registry for (searched anyway;
  see Component notes).

The 15 Fabric-migrated assets, their partitions, freshness, and automation
conditions are configured entirely from `defs/fabric_pipelines/defs.yaml` --
one component instance, one mapping table.

## Component notes

`DemoFabricWorkspaceComponent` (`components/fabric_pipelines_demo.py`)
subclasses the community registry's `fabric_workspace` component (rung 3 of
the escalation ladder) rather than writing one from scratch. Three registry
searches for the check layer (`"sql assertion check"`, `"data quality
check"`, `"duckdb check"`, plus `"custom check"` and `"row count check"`)
turned up data-quality frameworks pointed at pandas DataFrames or specific
warehouses (BigQuery, Great Expectations, Soda) -- nothing that fit a
DuckDB-native SQL assertion, which is core-Dagster `@asset_check` territory
regardless, not a registry gap.

For the legacy side, searched (per the brief's Component strategy) with
`--json`: `"external asset"`, `"observable source asset"`, `"observation
sensor"`, `"generic polling sensor"`, `"sql server change tracking"`,
`"database poller"`, `"file arrival sensor"`. Nothing vendor-neutral enough
for a bespoke in-house scheduler -- expected, since SFS's own scheduler has
no vendor to match against. A plain `AssetSpec` + hand-written `@sensor` is
core Dagster capability, not a rung-4 gap, per the brief.

Three real base-component limits on the Fabric side, each closed by
subclassing rather than by writing something from scratch -- see the full
writeup in the module docstring and
`component-feedback/2026-08-26-fabric-workspace-polling-sensor.md`:

1. **Live discovery** -- the documented seam (`_list_items()`); demo mode
   returns a fixed list instead of calling the Fabric REST API.
2. **Per-item asset keys / deps / partitions** -- the base component's
   `get_asset_spec(props)` hook exists for exactly this, but is only
   invoked when a `translation:` callable is also configured. This subclass
   calls it directly so the documented hook works as its own docstring
   describes.
3. **The polling sensor never actually built.** The base component declares
   a `polling_sensor` field and documents it, but nothing in
   `StateBackedComponent.build_defs` (or anywhere else in the base
   component) constructs a `SensorDefinition`. Setting `generate_sensor:
   true` changed nothing in the base component. The subclass builds the
   sensor itself, gated on the same flag.

## What's mocked vs. real

| Layer | Demo mode | Real mode |
|---|---|---|
| Fabric discovery (`_list_items`) | Fixed 15-item list from `assets_by_item_name` | Real `GET /workspaces/{id}/items` call -- flip `demo_mode: false` in `defs/fabric_pipelines/defs.yaml` |
| Fabric trigger-and-poll | Local synthetic-data generation with the same run/complete lifecycle | Real `POST .../jobs/instances` + poll, via the parent component's unmodified `_trigger_item_run` |
| Fabric-side polling sensor | Rotates through a fixed, deterministic mock run-history list | Would poll the same Fabric run-history endpoint, filtered to runs this component didn't trigger |
| Legacy scheduler run log | `demo_data/legacy_scheduler.py`, deterministic and seeded | SFS's real homegrown scheduler's own run log / database, polled the same way |
| Lakehouse + legacy storage | DuckDB (`demo_data/demo.duckdb`) | The real Fabric lakehouse tables the pipelines write to, plus SFS's real SQL Server storage for the legacy feeds |
| Workspace credentials | Not required -- `workspace_id` is a placeholder string | Real workspace GUID + `tenant_id`/`client_id`/`client_secret` (service principal with Contributor access) |

Every asset key, partition, check, freshness policy, and both sensors are
identical in both modes -- only the network/database boundary differs.

## Local vs. Dagster+

**Give the live demo locally with `dg dev`.** Dagster+ Serverless storage is
ephemeral -- a fresh container per run means the local DuckDB file does not
persist between runs there, so the "rematerialize one partition live" moment
and the legacy sensor's observation history only work reliably in `dg dev`.

**Dagster+ deployment proves the project is real**: the code location loads,
the graph renders with real lineage across both the legacy and migrated
halves, the component is genuine. Treat that as the proof point, not as the
place to click through the live sequence. If SFS wants cross-run persistence
in Dagster+, back the warehouse with something durable (S3-backed storage,
MotherDuck, or their real Fabric lakehouse / SQL Server).

## Three buckets

- **In code**: the 17-asset graph (2 legacy external assets + 15
  Fabric-migrated trigger-and-observe assets); `date` x `dealer_group`
  partitions; the three asset checks above (including one evaluated against
  externally-observed legacy data); freshness policies on
  `fact_delinquency_snapshot` and `abs_pool_eligibility`;
  `AutomationCondition.eager()` on the Fabric-migrated assets that should
  recompute themselves; the `bronze_ingestion_daily_6am` schedule; the
  subclassed `fabric_workspace` component with its own polling/observation
  sensor; `legacy_scheduler_observer`, the dedicated sensor for the two
  legacy assets; dynamic metadata passing between pipeline steps (the
  `as_of_date` and row-count values materialized into each asset's output
  metadata).
- **Handled by Dagster+ (demonstrate, don't build)**: Teams/email alerting
  on run failures, check failures, and freshness violations via native
  alert policies -- never a custom sensor; restart/re-run from point of
  failure; asset lineage visualization; asset health, run history, and
  duration-trend views; the backfill UI, as the platform surface behind the
  partition-scoped replay story.
- **Conversation only (mention, build nothing)**: AI-assisted operations,
  agentic monitoring, and "potentially self-healing workflows" (the AE
  notes' own phrase for a longer-term interest); a real embedded Power BI
  report or workspace integration (represented here as a single
  refresh-trigger asset only); the other ~683 SSIS packages beyond the two
  represented as legacy assets -- mention the scale, don't model each one;
  NDA/procurement process.

## Data realism and assumptions

SFS's real volumes, delinquency rates, and SLAs were not provided in the AE
notes (flagged as a gap in the brief). Cardinalities here (180-320 loan
originations/day, ~8% back-book delinquency rate, a 5,000-account serviced
back book split across four regions) are illustrative, chosen to be
plausible for a mid-size captive auto lender given the ~35-person data team
-- not sourced. Loan amounts, dealer counts, and credit scores are
synthetic, not real SFS data.

Assumptions made where the brief was silent: the overnight-batch cutoff the
dealer floorplan lateness check holds every region to (10:00, comfortably
after the 6am bronze schedule fires), the ABS pool eligibility threshold (a region's delinquency
rate under 20% of its serviced back book), typing all 15 Fabric items as
`DataPipeline` jobs (rather than splitting some out as Notebook/Dataflow
items) for a uniform trigger-and-observe story, and which two specific
packages are still legacy (dealer floorplan and credit bureau -- both
third-party-integration-heavy, a plausible last-to-migrate pair; the brief
flags this as a brief-level assumption, not an AE-confirmed fact).
