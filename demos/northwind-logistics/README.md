# Northwind Logistics — Dagster demo

A demo Dagster project built for Northwind Logistics, a mid-market 3PL. It
shows lineage-backed, checked data quality on top of their real pain: a
freight rate feed that lands late ~15% of the time, silently corrupting
margin-by-lane-and-customer numbers a new CFO now depends on, plus a
340-DAG Airflow estate where dbt failures get swallowed by a `BashOperator`
that exits 0.

## The narrative in one paragraph

Carrier rate data (FedEx, UPS, two regional LTL carriers) and shipment
events land daily. dbt Core builds staging and mart models on top,
culminating in `margin_by_lane_customer` — the number the CFO wants and
Priya's team can't currently trust. A blocking asset check,
`carrier_rate_arrival`, catches a carrier's rate data not showing up for a
day and stops `margin_by_lane_customer` from computing a silently-wrong
number for that carrier/lane/day — instead of green Airflow boxes hiding a
failed dbt run. Recovery is one partition: rematerialize the healed carrier's
day, and everything downstream recomputes on its own via declarative
automation. Nobody clicks `carrier_cost_allocation` or
`margin_by_lane_customer`.

## Asset graph

- **Ingestion** — `raw/carrier_rate_raw` (multi-partitioned: day x carrier),
  `raw/shipment_events_raw` (daily), `raw/salesforce_accounts`,
  `raw/zendesk_tickets`, `raw/netsuite_gl_entries` (via `dagster-fivetran`)
- **Staging** (dbt) — `staging/stg_carrier_rates`, `staging/stg_shipment_events`,
  `staging/stg_salesforce_accounts`, `staging/stg_zendesk_tickets`,
  `staging/stg_netsuite_gl_entries`
- **Marts** (dbt) — `marts/shipments_by_lane`, `marts/invoice_line_items`,
  `marts/carrier_cost_allocation`, `marts/invoice_billing_nightly`,
  `marts/margin_by_lane_customer`
- **Demo control** — `demo_control/healed_partitions`, the entire "heal the
  anomaly" surface (see below)

## Demo mode vs. real mode

Every component that touches an external system defaults to `demo_mode: true`
and generates deterministic synthetic data instead. Flipping to real mode is
a one-line change per `defs.yaml`, with real credentials supplied via
`{{ env.VAR_NAME }}`:

| Component | `defs.yaml` | Real mode points at |
|---|---|---|
| `CarrierRateFeedComponent` | `defs/ingestion/carrier_rate_raw/` | FedEx/UPS/regional-LTL rate APIs + `dagster-snowflake` for the warehouse write |
| `ShipmentEventsComponent` | `defs/ingestion/shipment_events_raw/` | The TMS event stream + `dagster-snowflake` |
| `DemoFivetranAccountComponent` | `defs/ingestion/fivetran_saas/` | A real Fivetran account (`dagster_fivetran.FivetranAccountComponent`, unmodified downstream of the state fetch) |
| `NorthwindDbtComponent` | `defs/transformation/*/` | Real dbt Core execution is **not mocked at all** — every mode runs the same dbt project. `demo_mode` here only controls a pre-build safety net (see below) |

Every custom component subclasses the real library component and fakes only
the network call that crosses into the external system — see
`templates/demo_mode_pattern.py` in the repo root for the pattern this
follows, and each component's own docstring for the specific seam.

**dbt is never mocked.** Real dbt Core executes against DuckDB in demo mode
and would execute identically against Snowflake in real mode (see
`dbt_project/profiles.yml`).

### Why the dbt component re-seeds the raw layer on every build

Dagster+ Serverless gives each run its own ephemeral disk — a local DuckDB
file written by one run is not guaranteed to be there for a later, separate
run. Since the recovery money-shot inherently spans multiple runs (heal →
rematerialize one partition → automation-triggered downstream runs),
`NorthwindDbtComponent.execute()` re-seeds the whole raw layer from the
deterministic generators (fast — small synthetic data) before every dbt
build, in demo mode only. This makes each run self-sufficient regardless of
which container it lands on. Real mode (`demo_mode: false`) skips this
entirely and relies on Snowflake's actual persistence.

## The planted anomaly and how to heal it

`regional_ltl_b`'s rate data never arrives for **2026-08-21**. The blocking
`carrier_rate_arrival` check catches it; `margin_by_lane_customer` for that
carrier/lane/day is absent rather than computed wrong.

**To heal it, entirely from the Dagster UI:**

1. Open the `demo_control/healed_partitions` asset and click **Materialize**.
2. In the launchpad config, set:
   ```yaml
   ops:
     demo_control__healed_partitions:
       config:
         healed: ["regional_ltl_b|2026-08-21"]
   ```
3. Rematerialize the single partition `regional_ltl_b|2026-08-21` on
   `raw/carrier_rate_raw`.
4. `staging/stg_carrier_rates`, `marts/carrier_cost_allocation`, and
   `marts/margin_by_lane_customer` recompute on their own via
   `AutomationCondition.eager()` — enable the default automation condition
   sensor under **Automation → Sensors** if it isn't already on.

**To reset the demo** (so the anomaly reappears), materialize
`demo_control/healed_partitions` again with `healed: []`.

State lives in Dagster's own event log (this asset's materialization
metadata), not a local file — see the component's docstring for why.

## Feature coverage

| Feature | Where |
|---|---|
| Partitions | `MultiPartitionsDefinition` (day x carrier) on `carrier_rate_raw`; daily elsewhere |
| Asset checks | `carrier_rate_arrival` (blocking), `invoice_batch_completeness`, plus 20 dbt-test-derived checks |
| Real dbt project | `dbt_project/` — 10 models, real `schema.yml` tests |
| Freshness policies | `margin_by_lane_customer`, `invoice_billing_nightly` |
| Automation conditions | `eager()` on `stg_carrier_rates`; `eager()` gated on upstream blocking checks passing for `carrier_cost_allocation` and `margin_by_lane_customer` |
| Schedule | `invoice_billing_nightly_schedule`, 5am ET (ahead of the 6am finance deadline) |
| Asset metadata | Row counts, `demo_mode` flag, carrier/date context on every ingestion asset |
| Groups and kinds | `ingestion` / `transformation` / `demo_control` groups; `python`, `api`, `fivetran`, `dbt` kinds |

## Getting started

### Installing dependencies

Ensure [`uv`](https://docs.astral.sh/uv/) is installed, then:

```bash
uv sync
source .venv/bin/activate
```

### Running Dagster

```bash
dg dev
```

Open http://localhost:3000. Enable the default automation condition sensor
under **Automation → Sensors** to see the recovery sequence recompute
automatically.

### Validating

```bash
dg check defs
dg check yaml
dg list defs
dg launch --assets '*' --partition <YYYY-MM-DD>
```

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
