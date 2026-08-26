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
migrated into Fabric pipelines. This demo proves Dagster can wrap SFS's
*actual* Fabric pipelines with lineage, blocking checks, freshness, and
partition-scoped execution, and that it doesn't require an all-or-nothing
cutover: a package still triggered by SFS's own scheduler mid-migration
shows up in the very same lineage graph.

## Running it

```bash
uv sync
dg dev
```

Open http://localhost:3000. **No environment variables or credentials are
required** -- the component defaults to demo mode, reading/writing a local
DuckDB file created on first run at
`src/stellantis_financial_services/demo_data/demo.duckdb`, standing in for
the Fabric lakehouse tables the real pipelines write to.

Everything runs via `dg`, never the legacy `dagster` CLI:

```bash
dg check defs                                                    # validate definitions
dg list defs                                                     # list every asset
dg launch --assets 'raw_loan_originations' --partition '2026-08-20'   # materialize one daily partition
dg launch --assets 'raw_dealer_floorplan_feed' --partition '2026-08-20|south'  # one region, one day
python validate_e2e.py                                           # full end-to-end harness (see below)
```

## The asset graph

One `DemoFabricWorkspaceComponent` instance
(`defs/fabric_pipelines/defs.yaml`) covers all 17 assets via an explicit
`assets_by_item_name` mapping table -- adding SFS's next migrated package is
one more entry in that table, not a new component instance or Python file
(the same shape as `assets_by_task_key` on the community registry's
Databricks workspace component). Every asset is a **trigger-and-observe
wrapper around a Fabric Data Pipeline item**, standing in for one of the
~700 SSIS packages -- Dagster triggers, tracks, and gates them; it does not
reimplement their transformation logic.

**Bronze** (`bronze_ingestion`, vendor-file ingestion, `date`-partitioned
unless noted): `raw_loan_originations`, `raw_lease_originations`,
`raw_payment_transactions`, `raw_credit_bureau_pull`, and
`raw_dealer_floorplan_feed` (`date` x `dealer_group` -- the one asset with a
genuine second partition dimension, since floorplan financing arrives
per-region).

**Silver** (`silver_transformation`, conforming logic): `stg_loan_originations`,
`stg_lease_originations`, `stg_payment_transactions`, `stg_delinquency_events`,
`dim_dealer` (rolls up all four `dealer_group` partitions of the floorplan
feed for one date via a `MultiToSingleDimensionPartitionMapping`), and
`dim_borrower`.

**Gold** (`gold_marts`): `fact_loan_portfolio`, `fact_delinquency_snapshot`
(freshness-policed), `abs_pool_eligibility` (the money-shot terminal asset --
freshness-policed + blocking-checked), `gl_reconciliation_summary`, and
`customer_360`.

**Reporting** (`reporting`): `powerbi_portfolio_dashboard_refresh` -- a
single Fabric-native Power BI refresh-trigger asset (see Three buckets below
for why it isn't a real embedded report).

17 assets total, matching the brief's target.

## Fidelity: data-backed

This demo needs real values on screen -- the reconciliation and completeness
checks compute their pass/fail from actual synthesized rows, the
dealer-floorplan lateness check compares a real arrival timestamp against an
SLA, and the ABS-pool and delinquency numbers look like a real loan
portfolio when asked "how many contracts is that." DuckDB stands in for the
Fabric lakehouse; generation is deterministic and seeded
(`demo_data/generators.py`), so row counts and dollar figures never drift
between runs.

## Asset checks (3)

| Check | Asset | Severity | Maps to |
|---|---|---|---|
| `raw_loan_originations_completeness` | `raw_loan_originations` | Blocking | "failure recovery is manual, replay is weak" |
| `abs_pool_eligibility_reconciliation` | `abs_pool_eligibility` | Blocking | 2026 ABS securitization calendar / investor loan-tape accuracy |
| `raw_dealer_floorplan_feed_lateness` | `raw_dealer_floorplan_feed` | Warning | "asset-level expected timing / lateness visibility" |

All three compute real conditions from the synthetic data and pass in this
demo -- they're built, wired, and visible so the room can see what they
assert and where the result surfaces, without needing to see one go red. A
fourth check (bronze-to-silver row-count reconciliation on payment
transactions) was in the brief as "optional if time allows" and was skipped
to keep scope disciplined; the three required checks above are fully real.

## Automation

- **Freshness policies**: `fact_delinquency_snapshot` and
  `abs_pool_eligibility` (6h fail / 3h warn) -- the direct answer to "how
  would we know something broke," paged against the ABS pool calendar.
- **Declarative automation**: `AutomationCondition.eager()` on every
  silver/gold/reporting asset (12 of 17) -- once bronze lands, everything
  downstream recomputes itself. The 5 bronze feeds are schedule-triggered
  instead (see below), since there's no upstream Dagster asset for them to
  react to -- a vendor file landing is the real-world trigger.
- **Schedule**: `bronze_ingestion_daily_6am` fires the four single
  `date`-partitioned bronze feeds at 6am -- the same overnight-batch cutoff
  the floorplan lateness check holds every region to.
- **Polling sensor**: `fabric_pipelines_external_run_observer` (opt-in via
  `generate_sensor: true`) detects Fabric pipeline runs Dagster did not
  trigger and emits `AssetObservation` events -- the direct answer to "what
  happens when it wasn't Dagster that started it," which for a
  homegrown-scheduler-migrating-to-Fabric shop is close to the whole point.
  See "Component notes" below for why this had to be built into the
  subclass rather than just configured.
- **No `RetryPolicy`** on any asset. Every source here is a Fabric pipeline
  triggered through the same Fabric REST API, not a genuinely flaky
  dealer-submitted feed with its own failure modes -- a decorative retry
  would invite a question ("what specifically is flaky here?") we'd lose.
  Skipped rather than padded, per CLAUDE.md.

## `defs/` file count

10 YAML files (the component instance + registry-installed component
metadata) vs. 6 Python files in `defs/` (3 asset checks + 1 schedule module
+ 2 `__init__.py`). Every `.py` file's justification:

- `defs/checks/*.py` (3 files) -- asset checks are core-Dagster
  `@dg.asset_check`-decorated Python functions; there is no YAML form for a
  custom SQL assertion, and no registry check component fit this domain
  (see Component notes -- three distinct registry searches came up empty for
  a SQL/DuckDB-native check).
- `defs/automation/bronze_ingestion_schedule.py` -- `define_asset_job` +
  `build_schedule_from_partitioned_job` are core-Dagster Python APIs with no
  YAML component wrapping them in this project.

All 17 assets, the sensor, partitions, freshness, and automation conditions
are configured entirely from `defs/fabric_pipelines/defs.yaml` -- one
component instance, one mapping table.

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

Three real base-component limits, each closed by subclassing rather than by
writing something from scratch -- see the full writeup in the module
docstring and `component-feedback/2026-08-26-fabric-workspace-polling-sensor.md`:

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
   true` changed nothing in the base component -- this is what sank the
   three prior builds of this demo (see `state/ledger.json`). The subclass
   builds the sensor itself, gated on the same flag.

## What's mocked vs. real

| Layer | Demo mode | Real mode |
|---|---|---|
| Fabric discovery (`_list_items`) | Fixed 17-item list from `assets_by_item_name` | Real `GET /workspaces/{id}/items` call -- flip `demo_mode: false` in `defs/fabric_pipelines/defs.yaml` |
| Fabric trigger-and-poll | Local synthetic-data generation with the same run/complete lifecycle | Real `POST .../jobs/instances` + poll, via the parent component's unmodified `_trigger_item_run` |
| Polling sensor | Rotates through a fixed, deterministic mock run-history list | Would poll the same Fabric run-history endpoint, filtered to runs this component didn't trigger |
| Lakehouse tables | DuckDB (`demo_data/demo.duckdb`) | The real Fabric lakehouse tables the pipelines write to |
| Workspace credentials | Not required -- `workspace_id` is a placeholder string | Real workspace GUID + `tenant_id`/`client_id`/`client_secret` (service principal with Contributor access) |

Every asset key, partition, check, freshness policy, and the sensor itself
are identical in both modes -- only the network boundary differs.

## Local vs. Dagster+

**Give the live demo locally with `dg dev`.** Dagster+ Serverless storage is
ephemeral -- a fresh container per run means the local DuckDB file does not
persist between runs there, so the "rematerialize one partition live" moment
only works reliably in `dg dev`.

**Dagster+ deployment proves the project is real**: the code location loads,
the graph renders with real lineage, the component is genuine. Treat that as
the proof point, not as the place to click through the live recovery
sequence. If SFS wants cross-run persistence in Dagster+, back the warehouse
with something durable (S3-backed storage, MotherDuck, or their real Fabric
lakehouse).

## Three buckets

- **In code**: the 17-asset trigger-and-observe Fabric pipeline graph;
  `date` x `dealer_group` partitions; the three asset checks above;
  freshness policies on `fact_delinquency_snapshot` and
  `abs_pool_eligibility`; `AutomationCondition.eager()` on 12 assets; the
  `bronze_ingestion_daily_6am` schedule; the subclassed `fabric_workspace`
  component with its polling/observation sensor turned on; dynamic metadata
  passing between pipeline steps (the `as_of_date` and row-count values
  materialized into each asset's output metadata).
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
  refresh-trigger asset only); NDA/procurement process.

## Data realism and assumptions

SFS's real volumes, delinquency rates, and SLAs were not provided in the AE
notes (flagged as a gap in the brief). Cardinalities here (180-320 loan
originations/day, ~8% back-book delinquency rate, a 5,000-account serviced
back book split across four regions) are illustrative, chosen to be
plausible for a mid-size captive auto lender given the ~35-person data team
-- not sourced. Loan amounts, dealer counts, and credit scores are
synthetic, not real SFS data.

Assumptions made where the brief was silent: the overnight-batch cutoff for
vendor feeds (6:00, informing both the schedule and the lateness check's
SLA), the ABS pool eligibility threshold (a region's delinquency rate under
20% of its serviced back book), and typing all 17 Fabric items as
`DataPipeline` jobs (rather than splitting some out as Notebook/Dataflow
items) for a uniform trigger-and-observe story.
