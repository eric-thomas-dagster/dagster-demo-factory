# Stellantis Financial Services -- demo

A Dagster demo modeling SFS's vendor-file -> Fabric pipeline -> ABS pool
reporting flow, built for a first tailored technical demo with Nick Gogos
(Enterprise Data Management lead) and Chris Rodriguez (enterprise data
platform lead) -- no meeting booked yet as of this build.

## The thesis

SFS is migrating ~700 homegrown SSIS packages onto Microsoft Fabric while
the data team triples in size. Their stated pain is that failure recovery is
manual and replay is weak in the system they built themselves, and with SFS
underwriting up to eight auto ABS securitizations in 2026, an audit-ready
loan-level tape isn't optional. They're evaluating Dagster as the
orchestration layer **on top of** their Fabric pipelines -- not as a new
transformation stack. This demo proves Dagster can wrap their actual Fabric
pipelines with lineage, blocking checks, freshness, and partition-scoped
execution, entirely green, and answers "replay is weak" with live
partition-scoped rematerialization rather than a staged break-and-fix.

## Running it

```bash
uv sync
dg dev
```

Open http://localhost:3000. **No environment variables or credentials are
required** -- every asset defaults to demo mode, reading/writing a local
DuckDB file created on first run at
`src/stellantis_financial_services/demo_data/demo.duckdb`.

Everything runs via `dg`, never the legacy `dagster` CLI:

```bash
dg check defs                                          # validate definitions
dg list defs                                            # list every asset
dg launch --assets raw_loan_originations --partition 2026-08-20     # one daily asset
dg launch --assets raw_dealer_floorplan_feed --partition '2026-08-20|midwest'  # one region/day
python validate_e2e.py                                  # full end-to-end harness (see below)
```

## The asset graph

**Bronze** (`bronze/*`, Fabric-triggered vendor ingestion) --
`raw_loan_originations`, `raw_lease_originations`, `raw_payment_transactions`,
`raw_credit_bureau_pull` (all daily), and `raw_dealer_floorplan_feed`
(`date x dealer_group` -- `midwest`/`northeast`/`south`/`west` -- the one
asset with a genuine second partition dimension and a retry policy, since
four independent regional dealer-network feeds are a genuinely flaky
source).

**Silver** (`silver/*`, Fabric-triggered conforming/staging) --
`stg_loan_originations`, `stg_lease_originations`, `stg_payment_transactions`,
`stg_delinquency_events`, `dim_dealer` (rolls up all four dealer_group
partitions for a date via `MultiToSingleDimensionPartitionMapping`),
`dim_borrower`.

**Gold** (`gold/*`, Fabric-triggered marts) -- `fact_loan_portfolio`,
`fact_delinquency_snapshot` (freshness-policed -- pages someone),
`abs_pool_eligibility` (freshness-policed + blocking check -- the money-shot
terminal asset, tied to SFS's 2026 ABS securitization calendar),
`gl_reconciliation_summary`, `customer_360`.

**Reporting** (`reporting/*`) -- `powerbi_portfolio_dashboard_refresh`
(represents the exec-facing dashboard refresh -- a single refresh-trigger
asset, not a real embedded report).

17 assets total. Every asset is a **trigger-and-observe wrapper around a
Fabric pipeline run** (`FabricPipelineAssetComponent`, see below) -- Dagster
triggers, tracks, and gates pipelines that already exist or are actively
being migrated into Fabric; it does not recompute their SSIS/stored-proc
transformation logic in a new engine.

## Components -- YAML-first

`defs/` is **24 `defs.yaml` files and zero hand-written per-asset Python**
(17 assets + 4 checks + 2 schedules + 1 resource = 24 component
instantiations; the only `.py` file anywhere under `defs/` is the required,
empty `defs/__init__.py`). All the Python lives in `components/`, written
once and instantiated repeatedly:

| Component | Purpose | Instantiated |
|---|---|---|
| `FabricPipelineAssetComponent` | One asset that triggers-and-observes one Fabric pipeline | 17x (every asset above) |
| `DuckDbAssetCheckComponent` | One SQL-assertion asset check (blocking or warning) | 4x (see below) |
| `PartitionedIngestionScheduleComponent` | A cron schedule over a partitioned asset job | 2x |
| `DemoFabricWorkspaceResourceComponent` | Demo-mode subclass of the registry's `fabric_workspace_resource` | 1x |

**Registry escalation:** `fabric_workspace` (imports live workspace items,
unpartitioned) and `fabric_pipeline_trigger_job` (bare job+schedule, not an
asset) don't fit a named/partitioned/checked asset graph -- confirmed by
`dagster-component search "microsoft fabric" "fabric pipeline trigger"
"fabric lakehouse" "fabric workspace" "power bi refresh"`. `fabric_workspace_resource`
(rung 2, registry as-is) supplies the REST client
(`list_items`/`trigger_item_run`/`wait_for_run`); `FabricPipelineAssetComponent`
wraps it as a demo-mode-able asset-producing component (rung 4 -- see
`component-feedback/2026-08-26-fabric-trigger-and-observe-asset.md` for the
registry gap this exposes). `DuckDbAssetCheckComponent` is rung 4 for the
same reason: no registry component expresses an arbitrary SQL assertion
against an asset materialized outside dbt.

No `.py` files hold per-asset logic in `defs/` -- every `defs.yaml` there is
config only.

## Partitioning

`DailyPartitionsDefinition` (`America/Detroit`, starting 2026-07-01) on 16 of
17 assets. `raw_dealer_floorplan_feed` carries a
`MultiPartitionsDefinition(date x dealer_group)` -- floorplan financing
genuinely arrives per region, so "rerun just the midwest partition for
8/20" is a real targeted-recovery story, not a talking point.

## Asset checks (4)

| Check | Asset | Severity | Maps to |
|---|---|---|---|
| `loan_originations_completeness` | `raw_loan_originations` | Blocking | "failure recovery is manual, replay is weak" |
| `abs_pool_eligibility_reconciliation` | `abs_pool_eligibility` | Blocking | 2026 ABS securitization calendar / loan-tape accuracy |
| `dealer_floorplan_feed_lateness` | `raw_dealer_floorplan_feed` | Warning | "asset-level expected timing / lateness visibility" |
| `payment_transactions_reconciliation` | `stg_payment_transactions` | Warning | "UI/visibility is fragmented ... hard to trust outputs" |

Plus `FreshnessPolicy`s on `fact_delinquency_snapshot` (6h fail / 3h warn)
and `abs_pool_eligibility` (12h fail / 6h warn). All four checks assert real
conditions computed from the synthetic data -- per current house rules
("demos always work"), the demo runs green end-to-end; checks are shown and
talked through, not triggered. **The AE's own notes ask to "show how
operators recover from a failed step without rerunning everything"; this
build answers that with live partition-scoped rematerialization against
clean data instead of a staged failure -- see the brief's Conflicts and
gaps section for why.**

## Automation

- **Schedules**: `bronze_daily_ingestion_job_schedule` (6:00am
  America/Detroit) covers the four daily bronze feeds; vendor files land by
  ~5am, so this clears the conformed layer before the 8am business start --
  a real deadline, not a placeholder cron. `dealer_floorplan_feed_job_schedule`
  (6:30am) covers the multi-partitioned floorplan feed separately, since a
  `define_asset_job` requires every selected asset to share one
  `partitions_def`.
- **Declarative automation**: `AutomationCondition.eager()` on every silver
  asset (`stg_*`, `dim_borrower`) -- once a day's bronze batch lands, the
  conforming layer recomputes on its own.
- **Alerting**: not built. Dagster+ ships native alert policies for Slack,
  Teams, email, and PagerDuty covering run failures, check failures,
  freshness violations, and schedule/sensor failures -- a better answer to
  the AE notes' "Teams/email notifications" ask than a hand-rolled sensor.
  Point at the alert policy UI and talk through wiring it to SFS's existing
  Teams channel.

`raw_dealer_floorplan_feed` carries a `RetryPolicy` (3 retries, 45s delay) --
the only asset in this build with one, since it's the only genuinely flaky
source (four independent regional dealer networks, not one deterministic
call). No retries elsewhere, per CLAUDE.md's guidance against decorative
retries on deterministic synthetic sources.

## What's mocked vs. real

| Layer | Demo mode | Real mode |
|---|---|---|
| Every Fabric pipeline trigger (17 assets) | Deterministic synthetic data (`demo_data/generators.py`), `FabricWorkspaceResource.trigger_item_run`/`wait_for_run` faked | Real Fabric REST trigger + poll -- flip `demo_mode: false` on `defs/resources/fabric/defs.yaml` and every asset's `defs.yaml`, and supply real `workspace_id`/tenant/client credentials |
| Lakehouse tables | Local DuckDB (`demo_data/demo.duckdb`) | Same DuckDB write path today -- reading the pipeline's real OneLake output back needs the workspace's lakehouse SQL endpoint, not yet wired (see `_trigger_and_observe`'s real-mode branch) |
| Asset checks | Real SQL assertions against DuckDB -- identical in both modes | Same, once the lakehouse tables point at real data |
| Alerting | Not built -- see Automation above | Dagster+ alert policy UI, no code change needed |

## Local vs. Dagster+

**Give the live demo locally with `dg dev`.** Dagster+ Serverless storage is
ephemeral -- a fresh container per run means the local DuckDB file does not
persist between runs there, so the "rematerialize one region's one day"
sequence only works locally.

**Dagster+ deployment proves the project is real**: the code location loads,
the 17-asset graph renders with `fabric`/`azure`/`powerbi` kind badges,
lineage is genuine. Treat that as the proof point, not the place to run the
live rematerialization sequence. If SFS wants Dagster+ to hold state across
runs, back the warehouse with something durable (S3-backed storage,
MotherDuck, or their real Fabric lakehouse).

## Data realism

SFS gave no real volumes (flagged as a gap in the brief) -- cardinalities
here (900-1,400 loan originations/day, 18k-24k payment transactions/day,
~700 dealers across 4 regions, a ~42k-contract servicing book growing ~6
contracts/day) are illustrative, chosen to be plausible for a mid-size
captive auto lender with a ~35-person data team, not sourced. All IDs,
dealer names, and balances are synthetic.
