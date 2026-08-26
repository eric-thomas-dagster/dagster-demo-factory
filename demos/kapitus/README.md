# Kapitus -- demo

A Dagster demo modeling Kapitus's small-business lending data flow, built
for a pricing/closing conversation with the CDAO and incoming VP, Data
Technology -- economic buyers who weren't hands-on in the Jul 22-Aug 18
technical POC that already ran directly against Dagster+.

## The thesis

Kapitus just brought on a new owner (InterVest Capital Partners) and is
mid-hire on a VP of Data Technology tasked with bringing fragmented,
Airflow-orchestrated pipelines into one governed strategy. The engineers are
already convinced -- the deal is in a live pricing conversation. This demo
gives the non-technical buyers a five-minute, narratable artifact that turns
"we tested it and it worked" into "here is the governed, observable lending
data platform Kapitus is buying," using Kapitus's own product-line
vocabulary so it reads as their platform.

## Running it

```bash
uv sync
dg dev
```

Open http://localhost:3000. **No environment variables or credentials are
required** -- every component defaults to demo mode, reading a local DuckDB
file created on first run at `src/kapitus/demo_data/demo.duckdb`.

Everything runs via `dg`, never the legacy `dagster` CLI:

```bash
dg check defs                                                          # validate definitions
dg list defs                                                           # list every asset
dg launch --assets '*' --partition '2026-08-18|term_loan'              # materialize one partition
python validate_e2e.py                                                 # full end-to-end harness (see below)
```

## The asset graph

**Ingestion** (`raw/*`, partitioned on `date x product_line`) -- three
bronze feeds: `loan_applications` (Fivetran-sourced from the loan
origination system, kinds `fivetran` + `aws`), `bank_statement_data`
(OCR-derived, S3-landed, kind `aws`), `credit_bureau_pulls` (commercial
bureau pull, S3-landed via Lambda, kind `aws`).

**Staging** (`staging/*`, dbt views + one seed, kinds `dbt` + `snowflake`) --
`stg_loan_applications`, `stg_bank_statement_data`, `stg_credit_bureau_pulls`,
`dim_borrower` (one row per business, latest underwriting signals),
`dim_product_line` (dbt seed -- Kapitus's five product lines and their shape).

**Marts** (`marts/*`, dbt tables) -- `funded_loans_daily` (the money-shot
mart, freshness-policed), `portfolio_performance_by_product`,
`credit_risk_summary`.

**Analytics** (`analytics/*`, dbt tables) -- `daily_funding_summary` (the
exec-facing rollup), `underwriting_decision_metrics` (funded-vs-declined
credit-score gap).

13 assets total. Real dbt Core runs against DuckDB for every staging/marts/
analytics model -- lineage, `schema.yml` tests, and asset checks are all
genuine, not simulated.

## Partitioning

`MultiPartitionsDefinition(date x product_line)` (`components/partitions.py`)
across every asset in the graph, per the brief -- the five product lines
(`term_loan`, `revenue_based_financing`, `equipment_financing`, `sba_loan`,
`line_of_credit`) are a genuine domain axis for Kapitus, not a fabricated
second dimension. This is what makes "rerun just the SBA partition for
8/12" a real targeted-recovery story rather than a talking point.

## Asset checks (3)

| Check | Asset | Severity | Maps to |
|---|---|---|---|
| `loan_applications_funded_amount_sanity` | `raw/loan_applications` | Blocking | "no unified data quality control" |
| `loan_applications_schema_stability` | `raw/loan_applications` | Warning | POC criterion: Fivetran schema-change alerts |
| `credit_bureau_pull_completeness` | `raw/credit_bureau_pulls` | Warning | Underwriting-risk data completeness |

Plus a `FreshnessPolicy` on `marts/funded_loans_daily` (30h fail / 24h warn)
-- the direct answer to "how would we know something broke," the VP Data
Technology req's core mandate. All three checks assert real conditions
computed from the synthetic data; per the brief (`Failure demonstration:
no`), the demo runs green end-to-end -- they're shown and talked through,
not triggered.

## Automation

- **Schedule**: `daily_bronze_ingestion_schedule` runs bronze ingestion at
  6am ET -- the one fixed-time trigger, matching the overnight batch cutoff.
- **Declarative automation**: every staging/marts/analytics asset carries
  `AutomationCondition.eager() & AutomationCondition.all_deps_blocking_checks_passed()`
  -- once a day's batches land, everything downstream recomputes on its own.
- **`credit_bureau_eventbridge_sensor`**: the POC's explicit "test invocation
  from EventBridge" criterion, represented as a real, configured, **stopped**
  sensor that would poll the SQS queue behind Kapitus's EventBridge
  notifications in real mode. No real AWS infrastructure is stood up (see
  Explicitly out of scope in the brief) -- registry search turned up
  `sqs_monitor`/`s3_monitor`, but both require a real queue/bucket with no
  demo-mode affordance (see `components/bronze_feed.py`'s docstring).
- **Alerting**: `critical_pipeline_failure_alert` (built-in
  `run_failure_sensor` -- the brief didn't name a channel, so this logs
  today and is a one-line swap to `dagster-slack`/`dagster-msteams` once
  Kapitus names theirs) monitors `critical_pipeline_job`
  (`funded_loans_daily` + everything upstream). Starts **stopped** --
  configured and visible in the UI, never making a network call during
  validation.

No `RetryPolicy` on any ingestion asset -- all three sources are
system-to-system (Fivetran, S3/Lambda), not dealer- or vendor-submitted, so
none of them are genuinely flaky in a way a decorative retry would honestly
represent. Skipped rather than padded, per CLAUDE.md.

## What's mocked vs. real

| Layer | Demo mode | Real mode |
|---|---|---|
| Bronze feeds (3 assets) | Deterministic synthetic data (`demo_data/generators.py`) | Real Fivetran/S3 fetch -- one line to flip: `demo_mode: false` in each `defs.yaml` under `defs/ingestion/` |
| S3 landing zone | Logged, no network call (`DemoS3Resource`) | Real upload -- `demo_mode: false` + real AWS credentials |
| EventBridge/SQS trigger | Sensor defined, stopped, no queue polled | `_DEMO_MODE = False` in `defs/automation/credit_bureau_eventbridge_sensor.py` + real boto3 SQS client + real credentials |
| Alerting | Logged, no external call / sensor stays stopped | Swap the `context.log.error` call for a real `dagster-slack`/`dagster-msteams` post + flip the sensor to running |
| dbt + DuckDB | **Real** -- actual dbt Core, actual models, actual lineage | Same dbt project against Snowflake -- change `KAPITUS_DBT_TARGET=live` + Snowflake connection env vars in `profiles.yml` |

## Local vs. Dagster+

**Give the live demo locally with `dg dev`.** Dagster+ Serverless storage is
ephemeral -- a fresh container per run means the local DuckDB file does not
persist between runs there.

**Dagster+ deployment proves the project is real**: the code location loads,
the graph renders, lineage is genuine. Treat that as the proof point. If
Kapitus wants Dagster+ to hold state across runs, back the warehouse with
something durable (S3, MotherDuck, or their real Snowflake warehouse).

## Data realism

Real Kapitus application volumes, approval rates, and product-line mix were
not provided in the AE notes (flagged as a gap in the brief). Cardinalities
here (15-45 applications per product line per day, ~72% approval rate) are
illustrative and round, chosen to be plausible relative to Kapitus's public
scale (~65,000 businesses funded cumulatively across 5 product lines) --
not sourced. Business IDs and credit scores are synthetic, not real Kapitus
borrower data.
