# Stellantis Financial Services -- demo

A Dagster demo modeling SFS's medallion (bronze -> silver -> gold) auto
loan/lease servicing pipeline, built for a technical demo with Nick Gogos
(Enterprise Data Management) and Chris Rodriguez (enterprise data platform).

## The thesis

SFS is migrating ~700 homegrown SSIS packages onto Microsoft Fabric while its
data team triples in size. Their stated pain: *"failure recovery is manual,
replay is weak, UI/visibility is fragmented."* With up to eight auto/lease
ABS securitizations planned for 2026, an audit-ready loan-level data tape is
not optional. This demo proves one thing: Dagster's asset graph gives them a
**partition-scoped, idempotent recovery story** -- rematerialize the one late
vendor file, not the whole 700-job chain -- plus the lineage and blocking
checks that keep bad loan data out of the securitization pool calculation.

## Running it

```bash
uv sync
dg dev
```

Open http://localhost:3000. **No environment variables or credentials are
required** -- every component defaults to demo mode, reading a local DuckDB
file created on first run at `src/stellantis_financial_services/demo_data/demo.duckdb`.

Everything runs via `dg`, never the legacy `dagster` CLI:

```bash
dg check defs                    # validate definitions
dg list defs                     # list every asset
dg launch --assets '*' --partition 2026-08-18   # materialize one day
python validate_e2e.py           # full end-to-end harness (see below)
```

## The asset graph

**Bronze** (`raw/*`, daily-partitioned, kind `azure`) -- five vendor file
drops: `loan_originations`, `lease_originations`, `payment_transactions`,
`dealer_floorplan_feed` (the one genuinely flaky, dealer-submitted source --
carries a real `RetryPolicy`), `credit_bureau_pull`.

**Silver** (`staging/*`, dbt views, kind `dbt` + `azure`) -- `stg_loan_originations`,
`stg_lease_originations`, `stg_payment_transactions`, `stg_delinquency_events`
(30+ days past due), `dim_dealer`, `dim_borrower`.

**Gold** (`marts/*`, dbt tables) -- `fact_loan_portfolio`, `fact_delinquency_snapshot`
(freshness-policed), `abs_pool_eligibility` (freshness-policed + blocking
check -- the money-shot terminal asset), `gl_reconciliation_summary`,
`customer_360`.

**Reporting** -- `reporting/powerbi_portfolio_dashboard_refresh`, a trigger
asset for the exec dashboard (kind `powerbi`).

17 assets total. Real dbt Core runs against DuckDB for every silver/gold
model -- lineage, `schema.yml` tests, and asset checks are all genuine, not
simulated.

## Partitioning: a deliberate simplification

The brief asked for a `MultiPartitionsDefinition` of `date x vendor_source`.
Each bronze asset already names one specific vendor feed, so `vendor_source`
would only ever take one value per asset -- a fake second dimension, not a
real one. This project uses a single `DailyPartitionsDefinition` instead
(`components/partitions.py`), matching SFS's daily-batch cadence. The
partition-scoped recovery story is unaffected: rematerializing one date on
one bronze asset never touches any other date or feed.

## Asset checks (4)

| Check | Asset | Severity | Maps to |
|---|---|---|---|
| `loan_originations_completeness` | `raw/loan_originations` | Blocking | "failure recovery is manual, replay is weak" |
| `dealer_floorplan_completeness` | `raw/dealer_floorplan_feed` | Blocking | The planted anomaly + recovery demo (see below) |
| `dealer_floorplan_lateness` | `raw/dealer_floorplan_feed` | Warning | "asset-level expected timing / lateness visibility" |
| `abs_pool_eligibility_completeness` | `marts/abs_pool_eligibility` | Blocking | 2026 ABS securitization calendar, investor/rating-agency scrutiny |

The brief named a blocking check on `raw_loan_originations` and a *warning*
lateness check on `raw_dealer_floorplan_feed`, but also described the
planted anomaly as a malformed record on `raw_dealer_floorplan_feed` that
must block downstream computation -- which needs a blocking check on that
asset. Both live on that feed: `dealer_floorplan_completeness` (blocking,
drives the recovery demo) and `dealer_floorplan_lateness` (warning, the
visibility ask). See `defs/checks/dealer_floorplan_checks.py`.

## The planted anomaly + recovery (money shot)

One `raw_dealer_floorplan_feed` partition (`2026-08-20`) arrives with a
malformed record -- an advance missing its VIN. This is real mock **source**
state (`demo_data/vendor_state.py`), not a Dagster object:

1. Materialize `raw_dealer_floorplan_feed` for `2026-08-20`. The batch lands
   with the malformed record. `dealer_floorplan_completeness` fails
   (blocking). `staging/dim_dealer` and everything downstream of it get no
   materialization event for that run -- Dagster genuinely skips them.
2. Run `python -m stellantis_financial_services.demo_data.simulate_corrected_feed`
   -- simulates the dealer resending a corrected file. This is an operation
   on the mock vendor source, never on Dagster: no heal asset, no reset job.
3. Rematerialize just `raw_dealer_floorplan_feed` for `2026-08-20`. The check
   passes. Rematerialize downstream (or let `AutomationCondition.eager()`
   pick it up) -- `dim_dealer` and every asset that depends on it recompute,
   for that one partition only. The other 699 packages' worth of work was
   never touched.

Reset the demo (both the "bad" and "corrected" mock states) with:

```bash
rm -f src/stellantis_financial_services/demo_data/_vendor_feed_state.json
```

## Automation

- **Schedule**: `daily_vendor_ingestion_schedule` runs bronze ingestion at
  5am ET -- the one fixed-time trigger, matching the real overnight vendor
  file cutoff.
- **Declarative automation**: every staging/marts/reporting asset carries
  `AutomationCondition.eager() & AutomationCondition.all_deps_blocking_checks_passed()`
  -- once bronze lands (or a corrected file arrives), everything downstream
  recomputes on its own. No manual re-triggering of the chain.
- **Alerting**: `critical_pipeline_teams_alert` (native `dagster-msteams`)
  monitors `critical_pipeline_job` (`abs_pool_eligibility` +
  `fact_delinquency_snapshot`). It starts **stopped** by default -- visible
  and configured in the UI, pointing at a placeholder webhook in demo mode,
  never making a network call during validation. An operator flips it on to
  demonstrate a live Teams alert on a run failure.

## What's mocked vs. real

| Layer | Demo mode | Real mode |
|---|---|---|
| Vendor file feeds (5 bronze assets) | Deterministic synthetic data (`demo_data/generators.py`) | Real vendor SFTP/API poll -- one line to flip: `demo_mode: false` in each `defs.yaml` under `defs/ingestion/` |
| ADLS/OneLake landing | Logged, no network call (`DemoADLS2Resource`) | Real upload -- `demo_mode: false` + real `storage_account`/`credential` |
| Teams alerting | Logged, no webhook call (`DemoMSTeamsResource`) / sensor stays stopped | `demo_mode: false` + real `hook_url`, flip the sensor to running |
| dbt + DuckDB | **Real** -- actual dbt Core, actual models, actual lineage | Same dbt project against Microsoft Fabric -- change `SFS_DBT_TARGET=live` + Fabric connection env vars in `profiles.yml` |
| Power BI dashboard refresh | Logged trigger, no API call | Real Power BI REST API call -- not implemented in this demo (explicitly out of scope per the brief) |

## Local vs. Dagster+

**Give the live demo locally with `dg dev`.** Dagster+ Serverless storage is
ephemeral -- a fresh container per run means the local DuckDB file does not
persist between runs there. The fail -> rematerialize -> recover sequence
spans multiple runs and only works reliably with `dg dev` on your machine.

**Dagster+ deployment proves the project is real**: the code location loads,
the graph renders, lineage is genuine. Treat that as the proof point, not as
where you click through the recovery sequence. If SFS wants Dagster+ to run
the actual recovery demo, back the warehouse with something durable (S3,
MotherDuck, or their real Fabric warehouse).

## Data realism

Real SFS loan volumes, dealer counts, and delinquency rates were not
provided (flagged as a gap in the brief). Cardinalities here are
industry-typical for a mid-size captive auto lender (~180 dealers,
60-140 loan originations/day, 900-1,500 daily payment transactions) --
illustrative, not sourced. Dealer names/regions in `dim_dealer` are derived
deterministically from the dealer_id, not real SFS dealership data.
