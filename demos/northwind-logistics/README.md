# Northwind Logistics — Dagster demo

## The pitch

Priya's four-person data team runs 340 undocumented Airflow DAGs where a
`BashOperator` swallows dbt failures and exits 0, so the first sign of a
broken pipeline is a customer emailing about a missing invoice. Freight
rate data from one of four carriers lands late ~15% of the time and
silently corrupts margin-by-lane-and-customer reporting until month-end
close catches it — right as a new CFO wants trustworthy numbers and a SOC2
renewal needs lineage evidence.

This demo shows: asset-level lineage that catches the late-carrier problem
with a check that fails loud (not a green box hiding a red dbt run);
targeted, partition-level recovery with a computed blast radius instead of
a manual backfill; and all of it addable incrementally next to Airflow, not
a rewrite.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required** — every demo-mode component defaults to a local DuckDB file at
`src/northwind_logistics/demo_data/demo.duckdb`, created on first
materialize.

## What's mocked vs. real

| Layer | Real mode | Demo mode (default) |
|---|---|---|
| Carrier rate APIs (FedEx/UPS/regional LTL) | Custom REST clients | Deterministic synthetic generator |
| Shipment events (TMS) | TMS event stream | Deterministic synthetic generator |
| Salesforce / Zendesk / NetSuite | Real Fivetran sync | Synthetic rows written straight to DuckDB |
| Warehouse | Snowflake (`dagster-snowflake`) | Local DuckDB (resource swap, same component) |
| dbt transformations | **Real dbt Core**, always | **Real dbt Core**, always — never mocked |

To go live against Northwind's actual stack: set `demo_mode: false` on the
three ingestion components in `src/northwind_logistics/defs/ingestion/*/defs.yaml`,
supply the `SNOWFLAKE_*` / `FIVETRAN_*` env vars those YAML files already
reference, and switch `dbt_project/profiles.yml`'s target from `demo` to
`live` (`NORTHWIND_DBT_TARGET=live`). No asset keys, partitions, checks, or
lineage change — that's the point of the demo-mode pattern.

## Which parts run where

- **The live, interactive demo (including the recovery sequence below)
  runs locally via `dg dev`.** The DuckDB file and the carrier-arrival
  state file are local filesystem state, and Dagster+ Serverless gives
  each run its own ephemeral disk — a sequence that spans separate runs
  (materialize today, rematerialize tomorrow) will not survive between
  Serverless runs.
- **Dagster+ is the proof point that the code location is real**: it
  loads, the graph renders, a single `dg launch --assets '*'` pass
  materializes cleanly in one run. Don't try to run the multi-step
  recovery sequence against the deployed location on a shared screen.

## The planted anomaly and the recovery sequence

`regional_ltl_b`'s rate feed has no data for `2026-08-21` the first time
it's read — modeled as source arrival timing (`demo_data/state.py`), not a
Dagster control object. The first read also marks that partition "arrived,"
so materializing it a second time picks up the data, exactly as a
rematerialize would in production.

1. `dg launch --assets '*'` (or **Materialize all** in the UI). The
   `carrier_rate_arrival` check fails, blocking, for `2026-08-21`.
   `margin_by_lane_customer` and `carrier_cost_allocation` for that day
   stay unmaterialized rather than publishing a wrong number.
2. Click into the failed check on `staging/carrier_rate_validated`. It
   reads as a business fact, not a stack trace.
3. Rematerialize **only** `raw/carrier_rate_raw`'s
   `regional_ltl_b|2026-08-21` partition. The source now has the data.
4. `carrier_rate_validated`, then `carrier_cost_allocation` and
   `margin_by_lane_customer` recompute **automatically** for `2026-08-21`
   via `AutomationCondition.eager()` — nobody clicks them. Enable the
   `default_automation_condition_sensor` under **Automation → Sensors**
   first.
5. Graph goes green, in under a minute.

Reset with `make reset-demo` (deletes the local DuckDB file and the
carrier-arrival state file — an operation on the mock source, never on a
Dagster object).

## Asset checks

- `carrier_rate_arrival` (**blocking**) on `staging/carrier_rate_validated`
  — the late-carrier-data check above.
- dbt schema tests (`not_null`, `unique`, `accepted_values`, `relationships`)
  across staging and marts models — real dbt test failures surface as real
  Dagster asset check failures, unlike the swallowed BashOperator exit code.
- `invoice_batch_completeness` on `marts/invoice_billing_nightly` —
  verifies the nightly batch has rows and a positive total before the 6am
  ET finance deadline.

## Automation

- `AutomationCondition.eager()` on `carrier_rate_validated`, so it
  recomputes the moment its raw partition updates.
- `AutomationCondition.eager() & all_deps_blocking_checks_passed()` on
  `carrier_cost_allocation` and `margin_by_lane_customer` — the check gate
  is what keeps them blocked while `carrier_rate_arrival` is failing.
- `invoice_billing_nightly_schedule` — runs the invoice chain at 5am ET,
  an hour ahead of the 6am finance deadline.

## Feature coverage

Partitions (multi-dimensional carrier x day on the raw feed, daily
downstream) · 3+ asset checks incl. one blocking · real dbt Core against
DuckDB with real generated lineage · Snowflake/Fivetran kind badges on
every asset that would run against them for real · freshness policies on
`invoice_billing_nightly` and `margin_by_lane_customer` · declarative
automation gated on blocking checks · a finance-deadline schedule ·
row-count/dollar metadata on checks and reporting models.

Not included: `dagster-airlift` (a talking point for the Airflow-migration
objection, not a build item per the brief); real carrier/Fivetran/Snowflake
credentials; a full 340-DAG or 600-model port.
