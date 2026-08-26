# Stellantis Financial Services -- demo

A Dagster demo built for SFS's Enterprise Data Management lead (Nick Gogos,
~3 months in role) and their platform lead (Chris Rodriguez), who are
evaluating Dagster as the orchestration layer over a ~700-package SSIS ->
Microsoft Fabric migration -- not asking Dagster to replace their
transformation logic.

## The thesis

SFS's homegrown orchestrator works, but "failure recovery is manual, replay
is weak," and SFS is underwriting up to eight auto ABS securitizations in
2026 -- an audit-ready, loan-level tape is not optional. This demo wraps
their *actual* Fabric pipelines (vendor file -> bronze -> silver -> gold ->
Power BI) with lineage, blocking checks, freshness, and partition-scoped
execution, every partition green, so Nick and Chris leave believing "Dagster
sits on what we're already building in Fabric, and doesn't ask us to rebuild
it."

## Running it

```bash
uv sync
dg dev
```

Open http://localhost:3000. **No environment variables or credentials are
required** -- `DemoFabricWorkspaceComponent` defaults to `demo_mode: true`,
reading/writing a local DuckDB file created on first run at
`src/stellantis_financial_services/demo_data/stellantis.duckdb`.

Everything runs via `dg`, never the legacy `dagster` CLI:

```bash
dg check defs                                                              # validate definitions
dg list defs                                                               # list every asset
dg launch --assets 'raw_loan_originations' --partition '2026-08-19'        # materialize one daily partition
dg launch --assets 'raw_dealer_floorplan_feed' --partition '2026-08-19|midwest'   # one dealer_group partition
python validate_e2e.py                                                     # full end-to-end harness (see below)
```

## The asset graph

Every asset is a **trigger-and-observe wrapper around a Fabric Data
Pipeline** -- Dagster triggers, tracks, and gates pipelines that already
exist (or are actively being migrated from SSIS), it does not recompute
their transformation logic in a new engine.

**Bronze** (vendor file ingestion) -- `raw_loan_originations`,
`raw_lease_originations`, `raw_payment_transactions`,
`raw_dealer_floorplan_feed` (the one `date x dealer_group`
multi-partitioned asset -- dealer floorplan feeds arrive per region),
`raw_credit_bureau_pull`.

**Silver** (conforming/staging -- the migrated SSIS logic, now Fabric
pipelines) -- `stg_loan_originations`, `stg_lease_originations`,
`stg_payment_transactions`, `stg_delinquency_events`, `dim_dealer` (rolls up
all 4 `dealer_group` partitions of the floorplan feed for one date via a
`MultiToSingleDimensionPartitionMapping`), `dim_borrower`.

**Gold** (marts, tied to the 2026 ABS calendar) -- `fact_loan_portfolio`,
`fact_delinquency_snapshot` (freshness-policed -- the asset someone pages
over), `abs_pool_eligibility` (freshness-policed + blocking check -- the
money-shot terminal asset), `gl_reconciliation_summary`, `customer_360`.

**Reporting** -- `powerbi_portfolio_dashboard_refresh`.

17 assets total, one `DemoFabricWorkspaceComponent` instance
(`defs/fabric_pipelines/defs.yaml`) with an explicit `assets_by_item_name`
mapping table binding all 17 -- adding SFS's 700th migrated pipeline is one
more table entry, not a new component instance or Python file.

## Partitioning

`DailyPartitionsDefinition` (start `2026-08-01`) on 16 of the 17 assets.
`raw_dealer_floorplan_feed` carries `MultiPartitionsDefinition(date x
dealer_group)` (`midwest`/`northeast`/`south`/`west`,
`components/partitions.py`) -- the one asset with a genuine second domain
axis, which is what makes the live rematerialization demo (below) a real
targeted-recovery story.

## Asset checks (4)

| Check | Asset | Severity | Maps to |
|---|---|---|---|
| `raw_loan_originations_completeness` | `raw_loan_originations` | Blocking | "failure recovery is manual, replay is weak" |
| `abs_pool_eligibility_reconciliation` | `abs_pool_eligibility` | Blocking | 2026 ABS calendar / audit-ready loan tape |
| `dealer_floorplan_feed_lateness` | `raw_dealer_floorplan_feed` | Warning | "asset-level expected timing / lateness visibility" |
| `payment_transactions_row_count_reconciliation` | `stg_payment_transactions` | Warning | "UI/visibility is fragmented... hard to trust outputs" |

All four pass on real, computed conditions every run -- see
`CLAUDE.md`/`LEARNINGS.md`: this demo never plants a failure. The checks are
built, wired, and visible so the prospect sees what they assert without
needing to watch one go red.

## Freshness, automation, and alerting

- `fact_delinquency_snapshot` and `abs_pool_eligibility` carry
  `FreshnessPolicy.time_window` (26-30h fail windows).
- `AutomationCondition.eager()` cascades from bronze through the reporting
  layer; `stg_loan_originations` and `abs_pool_eligibility` additionally
  gate on `all_deps_blocking_checks_passed()`.
- `overnight_vendor_file_schedule` (`defs/automation/defs.yaml`, a
  `cron_schedule` registry component) triggers the four daily-only bronze
  feeds at 00:00 UTC -- stopped by default so it never fires during
  validation. `raw_dealer_floorplan_feed` isn't on this schedule (its
  multi-partitioned job can't share a cron with the daily-only assets); it's
  materialized ad hoc / via backfill, which is also the money-shot asset
  below.
- **No custom alerting sensor** -- per the brief, Dagster+'s native alert
  policies (Slack/Teams/email/PagerDuty on run failures, check failures,
  freshness violations) cover SFS's stated Teams/email channel better than
  hand-rolled code would. Point at the Dagster+ alert policy UI.

## Component strategy -- rung 3, not rung 4

`DemoFabricWorkspaceComponent` (`components/fabric_workspace_demo.py`)
subclasses the registry's `fabric_workspace` component
(`components/fabric_workspace/component.py`, vendored unmodified). It
overrides exactly two network seams -- `_list_items` (discovery) and the
pipeline trigger/poll call -- and adds the `assets_by_item_name` mapping +
partitions support the parent lacks, per `templates/demo_mode_pattern.py`
and `CLAUDE.md`'s Rung 3 table. See the module docstring for the full
reasoning; this is the third build of this project, specifically to correct
a prior run that skipped this rung.

## Money shot (run-of-show)

1. Show the full asset graph, entirely green, `fabric`/`azure`/`powerbi`
   kind badges reading as SFS's actual stack.
2. Click `abs_pool_eligibility` -- freshness policy satisfied, the
   reconciliation check green. Narrate that it would block the downstream
   pipeline trigger on failure, without needing to show that live.
3. Rematerialize a single `raw_dealer_floorplan_feed` partition (one
   region, one date) via **Launchpad** or `dg launch`. It completes in
   seconds; show the eager automation condition on downstream siblings and
   narrate that only that region recomputes -- not the other three, not the
   other 699 packages' worth of pipeline.
4. Open `defs/fabric_pipelines/defs.yaml`'s `assets_by_item_name` mapping
   table -- adding SFS's 30th (or 700th) pipeline is one more entry, not a
   new Python class.

## What's mocked vs. real

- **Mocked:** the Fabric REST discovery and pipeline trigger/poll calls
  (`demo_mode: true` in `defs/fabric_pipelines/defs.yaml`). Flip to `false`
  and supply `AZURE_TENANT_ID`/`AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET` plus a
  real `workspace_id` to run against SFS's actual Fabric tenant -- asset
  keys, partitions, checks, and the YAML schema are unchanged.
- **Real:** DuckDB standing in for the lakehouse tables the Fabric pipelines
  write to; every asset check, freshness policy, and automation condition;
  the `cron_schedule` component.

## Local vs. Dagster+ Serverless

**Give the live demo locally with `dg dev`.** Dagster+ Serverless storage is
ephemeral -- each run is a fresh container, so the local DuckDB file doesn't
persist between Serverless runs. The Dagster+ deployment is the proof point
that the project loads and the graph renders for real, not the place to run
the multi-partition rematerialization sequence above.

## Data realism

Synthetic vendor data is deterministic (seeded, `demo_data/pipelines.py`) --
same seed, same row counts, every run. ~40 dealers across 4 regions,
150-400 loan originations/day, 900-1,800 payment transactions/day, a ~3%
30+-day delinquency rate -- illustrative order-of-magnitude for a mid-size
captive auto lender (the brief flags exact volumes as unknown/low-confidence;
don't present these as SFS's real numbers).
