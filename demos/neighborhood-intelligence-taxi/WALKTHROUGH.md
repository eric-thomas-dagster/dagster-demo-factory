# Manual Build Walkthrough — Neighborhood Intelligence Taxi Demo

Two ways to build this project:

- Hand [AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md) to Claude Code
- Walk the steps below by hand

Both produce the same graph. Both assume you've read
[PATTERNS.md](./PATTERNS.md) — the always-on rules (registry
components only, dbt-first, `post_processing:` before subclassing,
etc.) live there. This doc only holds what's *specific* to the NYC
Yellow Taxi build.

## Prereqs

- Python 3.10–3.14, [uv](https://docs.astral.sh/uv/)
- `dagster-component` CLI (installs on `uv sync` via
  `dagster-community-components-cli`)
- `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service account with
  `bigquery-public-data` read access
- Your own BigQuery project + dataset for dbt to write to
  (`NIT_BIGQUERY_PROJECT`, `NIT_BIGQUERY_DATASET`)

---

## Step 1 — Scaffold

```bash
uvx create-dagster project neighborhood-intelligence-taxi
cd neighborhood-intelligence-taxi
```

Add to `pyproject.toml` under `[project].dependencies`:

```toml
"dagster-dbt", "dbt-bigquery", "dagster-gcp",
"dagster-community-components-cli",
```

And the hatch wheel block (PATTERNS.md § Deploy conventions for the
why):

```toml
[tool.hatch.build.targets.wheel]
force-include = { "pyproject.toml" = "pyproject.toml" }
artifacts = [
    "src/neighborhood_intelligence_taxi/dbt_project/target/**",
    "src/neighborhood_intelligence_taxi/defs/.local_defs_state/**",
]
```

`uv sync`.

---

## Step 2 — Bronze — two external BigQuery tables

```bash
dagster-component add external_bigquery_table --auto-install
```

```yaml
# src/neighborhood_intelligence_taxi/defs/bronze/source_yellow_trips/defs.yaml
type: neighborhood_intelligence_taxi.components.ExternalBigQueryTableAsset
attributes:
  asset_key: source_yellow_trips
  project_id: bigquery-public-data
  dataset_id: new_york_taxi_trips
  table_id: tlc_yellow_trips_2022
  group_name: bronze
  partition_type: monthly
  partition_start: "2022-01-01"
  owners: ["team:nit-data-platform"]
  kinds: ["bigquery"]
  description: >-
    NYC TLC Yellow Taxi trips (2022) — public dataset.
  metadata:
    owner_team: "team:nit-data-platform"
    tier: "tier_1"
    domain: "trips"
```

Repeat for `source_taxi_zone_lookup` → `taxi_zone_geom`, `domain: reference`.

---

## Step 3 — dbt project (folders → groups)

Put the dbt project inside the Python package
(`src/<pkg>/dbt_project/`) so it ships in the wheel.

`dbt_project.yml` — groups per folder via `+group:`, declarations
in `_groups.yml` (PATTERNS.md § dbt conventions):

```yaml
name: "neighborhood_intelligence_taxi"
version: "1.0.0"
config-version: 2
profile: "neighborhood_intelligence_taxi"
model-paths: ["models"]
test-paths: ["tests"]

models:
  neighborhood_intelligence_taxi:
    silver: { +group: silver, +materialized: view,  +schema: silver }
    gold:   { +group: gold,   +materialized: table, +schema: gold }
    report: { +group: report, +materialized: table, +schema: report }
```

```yaml
# dbt_project/models/_groups.yml
version: 2
groups:
  - name: silver
    owner: { name: "NIT Data Platform", email: "data-platform@example.com" }
  - name: gold
    owner: { name: "NIT Data Platform", email: "data-platform@example.com" }
  - name: report
    owner: { name: "NIT Data Platform", email: "data-platform@example.com" }
```

```yaml
# dbt_project/profiles.yml
neighborhood_intelligence_taxi:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: service-account
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      project: "{{ env_var('NIT_BIGQUERY_PROJECT') }}"
      dataset: "{{ env_var('NIT_BIGQUERY_DATASET') }}"
      threads: 4
      location: "US"
```

Sources — the one place dbt has to know about Dagster (asset-key
remap onto bronze):

```yaml
# dbt_project/models/_sources.yml
version: 2
sources:
  - name: public_nyc_taxi
    database: bigquery-public-data
    schema: new_york_taxi_trips
    tables:
      - name: tlc_yellow_trips_2022
        meta: { dagster: { asset_key: ["source_yellow_trips"] } }
      - name: taxi_zone_geom
        meta: { dagster: { asset_key: ["source_taxi_zone_lookup"] } }
```

A representative model:

```sql
-- dbt_project/models/silver/stg_yellow_trips.sql
select
    vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
    trip_distance, pickup_location_id, dropoff_location_id, payment_type,
    fare_amount, tip_amount, tolls_amount, total_amount,
    date(pickup_datetime) as pickup_date,
    extract(hour from pickup_datetime) as pickup_hour
from {{ source('public_nyc_taxi', 'tlc_yellow_trips_2022') }}
where pickup_datetime is not null
  and dropoff_datetime is not null
  and pickup_location_id is not null
  and dropoff_location_id is not null
```

Silver / gold / report SQL follows the same pattern — `stg_taxi_zones`,
`fct_trips` (joins silver), three gold aggregates, two report models.
Ordinary dbt.

---

## Step 4 — Checks (dbt-native)

Column tests in each layer's `_schema.yml`:

```yaml
# dbt_project/models/gold/_schema.yml
models:
  - name: fct_trips
    data_tests:
      - equal_rowcount:
          other_model: ref('stg_yellow_trips')
          config: { severity: error }
    columns:
      - name: trip_id
        data_tests:
          - not_null: { config: { severity: error } }
          - unique:   { config: { severity: error } }
```

The `equal_rowcount` reference is a generic test macro (PATTERNS.md §
Testing conventions for why not a singular test):

```sql
-- dbt_project/macros/equal_rowcount_test.sql
{% test equal_rowcount(model, other_model) %}
    with a as (select count(*) as n from {{ model }}),
         b as (select count(*) as n from {{ other_model }})
    select a.n as this_rows, b.n as other_rows, a.n - b.n as diff
    from a, b
    where a.n != b.n
{% endtest %}
```

---

## Step 5 — Wire dbt as one Dagster component

Shared partitions_def + reusable freshness/automation factories:

```python
# src/neighborhood_intelligence_taxi/components/partitions.py
import dagster as dg

MONTHLY_PARTITIONS_DEF = dg.MonthlyPartitionsDefinition(start_date="2022-01-01")
```

```python
# src/neighborhood_intelligence_taxi/defs/transformation/template_vars.py
import datetime
import dagster as dg
from neighborhood_intelligence_taxi.components import MONTHLY_PARTITIONS_DEF


@dg.template_var
def monthly_partitions_def():
    return MONTHLY_PARTITIONS_DEF


@dg.template_var
def eager():
    return dg.AutomationCondition.eager()


@dg.template_var
def monthly_freshness():
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(days=32),
        warn_window=datetime.timedelta(days=28),
    )


@dg.template_var
def report_freshness_warning():
    return dg.FreshnessPolicy.time_window(
        fail_window=datetime.timedelta(days=32),
        warn_window=datetime.timedelta(days=28),
    )
```

The dbt component — one file for the whole dbt project (PATTERNS.md §
dbt conventions):

```yaml
# src/neighborhood_intelligence_taxi/defs/transformation/defs.yaml
type: dagster_dbt.DbtProjectComponent

template_vars_module: .template_vars

attributes:
  project:
    project_dir: ../../dbt_project
  translation:
    # group_name is NOT here — comes from dbt's +group config via manifest
    partitions_def: "{{ monthly_partitions_def }}"
    owners: ["team:nit-data-platform"]
    kinds: ["dbt", "bigquery"]
    metadata:
      owner_team: "team:nit-data-platform"
      tier: "tier_1"
      domain: "trips"

post_processing:
  assets:
    - target: "group:silver"
      attributes: { automation_condition: "{{ eager }}" }
    - target: "group:gold"
      attributes:
        automation_condition: "{{ eager }}"
        freshness_policy: "{{ monthly_freshness }}"
    - target: "group:report"
      attributes:
        automation_condition: "{{ eager }}"
        freshness_policy: "{{ report_freshness_warning }}"
```

`components/__init__.py` just re-exports the DCC classes so the UI's
Components tab lists them:

```python
from neighborhood_intelligence_taxi.components.partitions import MONTHLY_PARTITIONS_DEF
from neighborhood_intelligence_taxi.components.cron_schedule import CronScheduleComponent
from neighborhood_intelligence_taxi.components.external_bigquery_table import ExternalBigQueryTableAsset

__all__ = ["MONTHLY_PARTITIONS_DEF", "CronScheduleComponent", "ExternalBigQueryTableAsset"]
```

Zero custom components.

---

## Step 6 — Schedule

```bash
dagster-component add cron_schedule --auto-install
```

```yaml
# src/neighborhood_intelligence_taxi/defs/automation/defs.yaml
type: neighborhood_intelligence_taxi.components.CronScheduleComponent
attributes:
  schedule_name: nit_monthly_full_pipeline_schedule
  cron_expression: "0 6 1 * *"
  execution_timezone: UTC
  default_status: STOPPED
  job_name: nit_monthly_full_pipeline_job

  partition_type: monthly
  partition_start: "2022-01-01"

  asset_keys:
    - silver/stg_yellow_trips
    - silver/stg_taxi_zones
    - gold/fct_trips
    - gold/agg_daily_zone
    - gold/agg_hourly_demand
    - gold/agg_vendor_performance
    - report/report_daily_summary
    - report/report_top_zones

  tags:
    team: nit-data-platform
    schedule_role: monthly_full_pipeline
```

---

## Step 7 — Load + deploy

```bash
export NIT_BIGQUERY_PROJECT=your-gcp-project
export NIT_BIGQUERY_DATASET=nit_taxi
dg check defs
dg utils refresh-defs-state   # required for dbt after edits (see PATTERNS.md)
dg dev
```

Ship to Dagster+ (from factory root):

```bash
./scripts/deploy_demo.sh neighborhood-intelligence-taxi neighborhood_intelligence_taxi
```

---

## What the demo talks about

- **Manual-build story** — walk through this document. "Zero custom
  components. dbt is the source of truth for the transformation layer;
  Dagster reads it and adds partitions, freshness, and scheduling on
  top."
- **AI-driven build story** — show
  [AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md) as the input, and say
  "same graph, one prompt referencing PATTERNS.md."
- **The reuse story** — [PATTERNS.md](./PATTERNS.md) is yours. Every
  future NI project starts from the same brief template + a
  project-specific AI prompt. This build was the first.
- **The demo itself** — click through the Dagster UI. Show the bronze
  external-asset lineage into the dbt-generated silver/gold/report
  layers. Point at the checks and freshness policies. Point at the
  monthly schedule. Alerting is a Dagster+ platform feature (native
  alert policies for Slack / Teams / email / PagerDuty) — talk about
  it, don't build it.
