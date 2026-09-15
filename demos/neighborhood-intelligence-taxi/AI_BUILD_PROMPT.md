# AI Build Prompt — Neighborhood Intelligence Taxi Demo

Copy-paste the prompt below into a Claude Code session (with the Dagster
skills installed) running in an empty directory to build this project.

The prompt references [PATTERNS.md](./PATTERNS.md) for how any Dagster
project at NI should be structured — this file only carries the
*specifics* of this particular pipeline. Every future NI project uses
the same template: point at PATTERNS.md, add the per-project brief.

## Setup

```bash
npm install -g @anthropic-ai/claude-code
claude plugin install dagster/dagster-expert
claude plugin install dagster/dagster-integrations
claude plugin install dagster/dignified-python
```

## The prompt

```
Read PATTERNS.md at the project root — it defines how every Dagster
project at Neighborhood Intelligence is structured (registry components
only, dbt-first for transformations, one DbtProjectComponent instance
per project, groups from dbt +group config, checks as dbt generic test
macros not singular tests, post_processing before subclassing, etc.).
Follow those patterns.

Then scaffold this specific project:

## Project

- Slug: `neighborhood-intelligence-taxi`
- Package: `neighborhood_intelligence_taxi` (underscore)
- Story: bronze → silver → gold → report over the NYC Yellow Taxi
  public dataset in BigQuery. Ingestion is out of scope — data lives
  in `bigquery-public-data.new_york_taxi_trips`. Assume
  `GOOGLE_APPLICATION_CREDENTIALS` is set with read access.

## Bronze — external tables

Two `external_bigquery_table` instances:

| Asset key | project_id | dataset_id | table_id |
|---|---|---|---|
| `source_yellow_trips` | bigquery-public-data | new_york_taxi_trips | tlc_yellow_trips_2022 |
| `source_taxi_zone_lookup` | bigquery-public-data | new_york_taxi_trips | taxi_zone_geom |

`group_name: bronze`, `kinds: ["bigquery"]`,
`partition_type: monthly`, `partition_start: "2022-01-01"`,
`owners: ["team:nit-data-platform"]`, metadata `owner_team`, `tier: tier_1`,
`domain: trips|reference`.

## dbt models

Three groups (declared in `models/_groups.yml`, tagged via `+group:` per
folder in `dbt_project.yml`):

- **silver** (materialized: view): `stg_yellow_trips` (typed + null
  filter), `stg_taxi_zones`
- **gold** (materialized: table): `fct_trips` (joins silver → fan-in),
  `agg_daily_zone`, `agg_hourly_demand`, `agg_vendor_performance`
- **report** (materialized: table): `report_daily_summary`,
  `report_top_zones` (fan-out from gold)

`_sources.yml` uses `meta.dagster.asset_key` to remap the two dbt
sources onto the bronze asset keys.

## Checks

- Column tests in each layer's `_schema.yml` — `not_null` on every
  join key; `unique` on `fct_trips.trip_id` (blocking) and other
  natural keys.
- **Row-count reconciliation** between silver and gold as a **generic
  test macro** at `macros/equal_rowcount_test.sql`, referenced from
  `fct_trips` in `gold/_schema.yml` via
  `data_tests: - equal_rowcount: other_model: ref('stg_yellow_trips')`.
  Same shape as `dbt_utils.equal_rowcount` without adding the package.
  Blocking. (Reminder from PATTERNS.md: singular tests in `tests/*.sql`
  don't surface as asset checks — generic-test macros do.)
- Freshness on the gold and report layers via
  `post_processing.attributes.freshness_policy` — `32d fail / 28d warn`
  time_window matching the monthly cadence.

## Partitions + schedule

- `MonthlyPartitionsDefinition(start_date="2022-01-01")` shared across
  bronze + all dbt layers via `template_vars.py`.
- One `cron_schedule` component instance:
  - `schedule_name: nit_monthly_full_pipeline_schedule`
  - `partition_type: monthly`, `partition_start: "2022-01-01"`
  - `cron_expression: "0 6 1 * *"` (06:00 UTC on the 1st)
  - `default_status: STOPPED`
  - `asset_keys:` lists only executable assets (silver + gold + report,
    8 total). Bronze external assets are AssetSpec-only and can't be
    job members.

## Kinds (visual fidelity)

- bronze → `["bigquery"]`
- silver / gold / report (dbt) → `["dbt", "bigquery"]` via the dbt
  component's `translation:` block

## Report at the end

1. YAML-to-Python file count in `defs/`.
2. Every registry component ID used with its full path.
3. Any subclass, with the specific missing-config-field justification
   and why `post_processing:` + first-class YAML fields aren't enough.
4. `dg check defs` output.
```

## Alternative — hand this to Claude via MCP

Same prompt, delivered to Claude through the Dagster+ MCP server at
`mcp.agent.dagster.cloud/mcp/`. The MCP surface gives Claude live
access to the workspace state (existing definitions, code locations,
run history), which is helpful when iterating on an existing project
rather than a fresh scaffold.
