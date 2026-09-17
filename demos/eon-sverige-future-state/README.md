# E.ON Sverige future-state demo

**Demo name:** "Fragmented Today, One Graph Tomorrow"

A Dagster demo for E.ON Sverige (Swedish energy utility) built around
their stated future-state architecture:

```
Source Data (ADLS) -> Databricks -> dbt -> Curated Data Product -> Azure ML -> Business/ML Output
```

## Zero-setup run

```bash
uv sync
uv run python validate_e2e.py    # green out of the gate
uv run dg dev                     # UI at http://localhost:3000
```

No env vars required. Demo mode is the default (`demo_mode: true` in
every component). Flip components' `demo_mode: false` and supply
real credentials to run against actual Databricks + Azure ML MLflow.

## Asset graph (16 assets)

| Group | Assets | Kind badges |
|---|---|---|
| `source_data` | `source/raw_meter_reads`, `source/raw_grid_load_telemetry`, `source/raw_customer_records` | `azure`, `adls` |
| `databricks_ingestion` | `bronze/meter_reads_delta`, `bronze/grid_load_telemetry_delta`, `bronze/customer_records_delta`, `bronze/bronze_dq_gate` | `databricks`, `python` |
| `dbt_transformation` | `silver/stg_meter_reads`, `silver/stg_grid_telemetry`, `gold/fct_meter_daily`, `gold/fct_grid_hourly`, `gold/dim_customer_current` | `dbt`, `databricks` |
| `curated_data_products` | `curated/customer_switching_extract` | `dbt`, `databricks` |
| `azure_ml` | `grid_load_forecast_model`, `meter_anomaly_predictions` | `azure_ml`, `mlflow` |
| `business_output` | `grid_operations_daily_briefing` | `azure`, `power_bi` |

Everything is monthly-partitioned from `2026-04-01`.

## The demo thesis

E.ON's biggest pain is fragmentation across Databricks + dbt with
troubleshooting spread across GitLab schedules + Databricks native
orchestration. The `databricks_job_observation_sensor` is the primary
pain-addressing feature -- it emits `AssetObservation` events for runs
Dagster did not trigger (i.e., E.ON's GitLab scheduler still fires
these jobs today; Dagster observes what actually ran). Coexistence,
not migration.

The `customer_switching_extract` is the EU 2026/855 audit-trail moment
in the demo -- every column carries upstream lineage from raw source
through Databricks + dbt.

## Three buckets

- **In code:** the asset graph, 30+ dbt-native tests, 1 generic
  reconciliation test (`equal_rowcount` fct_meter_daily <-> stg_meter_reads,
  blocking severity), freshness policies on dbt marts + on the business
  output, automation conditions, monthly cron schedule, Databricks
  observation sensor.
- **Handled by Dagster+:** alerting (Slack / Teams / email / PagerDuty)
  on check failures, restart-from-failure, lineage visualization, run
  history + duration trends. Show these in the UI; do not build.
- **Conversation only:** actual Landis+Gyr Gridstream Connect API
  wiring, real Azure ML tracking-server endpoint, real Power BI
  destination for the briefing, regulator delivery format for EU
  2026/855.

## Components used

### Registry (community or native, YAML-driven)

- `dagster_dbt.DbtProjectComponent` -- subclassed as `EonDbtComponent`
  to badge dbt assets with `{dbt, databricks}` kinds instead of
  DuckDB's default (dagster-dbt derives from adapter_type).
- `dagster_community_components.CronScheduleComponent` -- monthly
  full-pipeline schedule, partitioned-job path, default STOPPED.

### Custom (rung 4)

- `DatabricksJobsWorkspaceComponent` -- enumerate + execute + observe
  triad for Databricks Jobs. No community component fit (see
  `component-feedback/2026-09-04-databricks-jobs-workspace-demo-mode-
  and-observation.md`). Built to the workspace-component convention
  (`@public get_asset_spec(props)`, `polling_sensor` field, mockable
  execution seam).
- `DemoMLflowModelPromotionComponent` /
  `DemoMLflowModelInferenceComponent` -- reimplementations of the
  community `MLflowModelPromotionComponent` /
  `MLflowModelInferenceComponent` with a `demo_mode` seam. Same YAML
  surface; short-circuits `mlflow` calls when demo_mode is true so the
  demo has no `mlflow` runtime dep.

## Real-mode switch

Every demo-mode field has a real-mode counterpart:

- `defs/databricks/defs.yaml`: set `demo_mode: false`,
  `workspace_host: {{ env.DATABRICKS_HOST }}`,
  `workspace_token: {{ env.DATABRICKS_TOKEN }}`. Fill each job's
  `job_id: "<numeric id>"`.
- `defs/mlflow/*/defs.yaml`: set `demo_mode: false`. Set
  `MLFLOW_TRACKING_URI` env var to Azure ML's MLflow-compatible
  endpoint.
- `src/eon_sverige_future_state/dbt_project/profiles.yml`: set
  `EON_DBT_TARGET=prod` and provide Databricks connection env vars.
