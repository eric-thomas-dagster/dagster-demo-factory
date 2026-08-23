# Northwind Logistics — Dagster demo

Built for the 2026-08-24 Dagster+ demo with Priya Raghunathan (Dir. Data
Engineering) and Marcus Boyd (Staff Data Engineer). See `../../briefs/2026-08-24-northwind-logistics.md`
in the monorepo for the full brief this demo was built from.

## The thesis

Northwind's four-person data team runs ~340 undocumented Airflow DAGs where
dbt failures get swallowed by a `BashOperator` that exits 0 -- the first
signal of a broken pipeline is a customer emailing about a missing invoice.
Freight rate data from one carrier lands late ~15% of the time and corrupts
margin-by-lane reporting until month-end close catches it, right as a new CFO
is demanding trustworthy numbers and a SOC2 renewal is coming up on lineage.

This project shows: a check that fails loud (not a green box hiding a red dbt
run); recovery as one partition with a computed blast radius, not a manual
backfill; and all of it addable incrementally, not a 340-DAG rewrite.

## The asset graph

**Ingestion** (`carrier_rate_raw`, `shipment_events_raw`,
`salesforce_accounts`, `zendesk_tickets`, `netsuite_gl_entries`) lands raw
data in the warehouse. **Validation** (`carrier_rate_validated`,
`shipment_events_clean`) is where the blocking arrival check lives.
**Transformation** (`shipments_by_lane`, `invoice_line_items`,
`carrier_cost_allocation`, `carrier_performance_summary`,
`customer_shipment_summary`) builds the lineage a technical audience can
trace end to end. **Reporting** (`invoice_billing_nightly`,
`margin_by_lane_customer`) is what the CFO and Priya's team actually read.

`carrier_cost_allocation` and `margin_by_lane_customer` carry
`AutomationCondition.eager()` -- they recompute on their own the moment their
inputs change. `invoice_billing_nightly` runs on a fixed 6am ET schedule
instead, because finance's close deadline is a clock, not a data dependency.

## Run-of-show

1. **Open the asset graph.** The `2026-08-21` partition of
   `carrier_rate_validated` is materialized, but `margin_by_lane_customer`
   for that date is not -- it never computed, it isn't wrong. *"Their Airflow
   shows green right here."*
2. **Click the `carrier_rate_arrival` check.** It reads as a business fact:
   *"Regional LTL carrier B rate data has not arrived for this partition;
   downstream margin is blocked, not silently wrong."*
3. **Trace lineage** from `carrier_rate_validated` through
   `carrier_cost_allocation` / `invoice_line_items` to `margin_by_lane_customer`.
   That trace is the SOC2 evidence Priya deferred on last cycle.
4. **Heal it.** Run `heal_carrier_rate_partition_job` (from the UI: Jobs →
   `heal_carrier_rate_partition_job` → Launchpad → set `rate_date:
   "2026-08-21"` → Launch). No YAML edit, no terminal.
5. **Rematerialize `carrier_rate_raw` for `2026-08-21` only** -- one asset,
   one partition. *"In Airflow this is clearing a DAG run and guessing the
   blast radius. Here the partition is the unit of recovery."*
6. **Watch it cascade.** `AutomationCondition.eager()` recomputes
   `carrier_cost_allocation` and `margin_by_lane_customer` automatically.
   Nobody clicks them. Graph goes green, under a minute.

## What's mocked vs. real

| Layer | Real | Mocked |
|---|---|---|
| dbt Core | Yes -- real `dbt build` against a real (DuckDB) warehouse | -- |
| Warehouse | -- | `dagster_snowflake.SnowflakeResource`, subclassed to redirect to DuckDB (`DemoSnowflakeResource`, `demo_mode: true`) |
| Fivetran (Salesforce/Zendesk/NetSuite) | -- | `dagster_fivetran.FivetranAccountComponent`, subclassed to serve synthetic connector state and syncs (`DemoFivetranAccountComponent`) |
| Carrier rate APIs, shipment event stream | -- | Custom components; no real carrier-API integration is in scope for this build (see "Explicitly out of scope" below) |

**To flip the warehouse to real Snowflake:**
1. The shared `warehouse` resource (`defs/resources.py`) is built from
   environment variables, not YAML -- set `NORTHWIND_DEMO_MODE=false` plus
   real `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` /
   `SNOWFLAKE_WAREHOUSE` / `SNOWFLAKE_DATABASE` env vars. Every asset that
   depends on `warehouse` (the two ingestion components) picks this up with
   no code or YAML change.
2. For dbt: `export DBT_TARGET=prod` plus the same `SNOWFLAKE_*` env vars
   (see `dbt_project/profiles.yml`).

**To flip Fivetran to a real account:** set real `FIVETRAN_ACCOUNT_ID` /
`FIVETRAN_API_KEY` / `FIVETRAN_API_SECRET` env vars and `demo_mode: false`
in `src/northwind_logistics/defs/fivetran_saas/defs.yaml`.

## The planted anomaly

`regional_ltl_b` is missing entirely from `carrier_rate_raw` for the
`2026-08-21` partition until healed (see `demo_data/generators.py`). The
`carrier_rate_arrival` dbt test fails for that partition, and because
`dbt build` skips every downstream node when an upstream test fails,
`invoice_line_items`, `carrier_cost_allocation`, `carrier_performance_summary`,
`customer_shipment_summary`, `invoice_billing_nightly`, and
`margin_by_lane_customer` all simply don't compute for that date -- rather
than computing a wrong number. `invoice_batch_completeness` on
`invoice_billing_nightly` is a second, independent check mapping to "we find
out something broke when a customer emails about a missing invoice."

## Resetting the demo

The anomaly is seeded deterministically, so a naive rematerialize regenerates
the same missing data. Healed-partition state lives in
`src/northwind_logistics/demo_data/.demo_state/healed_partitions.json`
(gitignored, machine-local). To run the demo again from a broken state:

```bash
dg launch --job reset_demo_job
```

Or from the UI: Jobs → `reset_demo_job` → Launch.

## Scope decisions and thin ice

- **`carrier_rate_raw` uses daily time partitions with carrier as a column**,
  not a `MultiPartitionsDefinition` over date x carrier. A multi-partitioned
  upstream feeding daily-partitioned dbt models downstream needs a custom
  partition mapping between the two, which was judged too costly for this
  build window. This means recovery granularity is "one day" rather than
  "one carrier on one day" -- rematerializing `carrier_rate_raw` for
  `2026-08-21` regenerates all four carriers' rows for that day, not just
  `regional_ltl_b`'s. Still correct, just a coarser blast radius than the
  ideal version.
- **Real carrier-API and event-stream ingestion are out of scope.**
  `CarrierRateFeedComponent` and `ShipmentEventsFeedComponent` raise
  `NotImplementedError` if `demo_mode: false` -- there is no registry or
  native Dagster component for arbitrary carrier rate APIs (confirmed via
  `dagster-component search`), and building one was out of scope per the
  brief.
- **`dagster-airlift`** (the answer to "what about our 340 DAGs") is a
  talking point, not a build item, per the brief -- it isn't wired into this
  project.
- **The Fivetran demo-mode subclass fakes two seams**, not one:
  `write_state_to_path` (the discovery-time API call a real Fivetran account
  would answer) and `execute` (the sync-time call). This is more surface area
  to fake than a typical demo-mode component, because `FivetranAccountComponent`
  is a `StateBackedComponent`. Worth a careful look before demoing live.
- **`invoice_billing_nightly`'s GL reconciliation figure is a whole-ledger
  running total**, not scoped to the billing date -- NetSuite GL entries
  aren't shipment-date-scoped in this synthetic data. Fine for an eyeball
  reconciliation in the room; don't imply it's an exact per-day match.

## Running locally

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. Materialize a partition from the asset graph, or
run `dagster asset materialize --select '*' -m northwind_logistics -p <date>`
from the CLI (dates `2026-08-17` through the most recent available daily
partition).
