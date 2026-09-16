# Manual Build Walkthrough — Pure Python (no dbt, no DCC)

Same graph as [WALKTHROUGH.md](./WALKTHROUGH.md), built with only
`dagster` + `dagster-gcp`. No dbt, no Dagster Community Components.
This is what a Dagster engineer types into an empty project when
they haven't adopted a transformation framework — the language of
Dagster itself, no other tools.

**Why this file exists:** the dbt + DCC path (see WALKTHROUGH.md) is
gorgeous when you've adopted them. If you haven't, this is what the
raw Dagster experience looks like — and the contrast is the point.
Same behavior, more code. That's the trade you're evaluating when you
consider adopting dbt or leaning on the community registry.

## Prereqs

- Python 3.10–3.14, [uv](https://docs.astral.sh/uv/)
- `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service account with
  read access to `bigquery-public-data` and write access to your own
  BigQuery dataset (`NIT_BIGQUERY_PROJECT`, `NIT_BIGQUERY_DATASET`)

## Project layout

```
neighborhood-intelligence-taxi-python/
├── pyproject.toml
├── src/neighborhood_intelligence_taxi/
│   ├── __init__.py
│   ├── definitions.py             # top-level wiring
│   ├── partitions.py              # shared MonthlyPartitionsDefinition
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── bronze.py              # 2 AssetSpecs
│   │   ├── silver.py              # 2 @asset functions
│   │   ├── gold.py                # 4 @asset functions
│   │   └── report.py              # 2 @asset functions
│   ├── checks.py                  # @asset_check for reconciliation
│   └── schedules.py               # @schedule
```

Every asset is one hand-written Python function. Every partition
window is a SQL `WHERE` clause you write yourself. Every check is a
Python function that opens a BigQuery client and runs queries.

## Step 1 — Scaffold

```bash
uvx create-dagster project neighborhood-intelligence-taxi-python
cd neighborhood-intelligence-taxi-python
```

Add to `pyproject.toml`:

```toml
[project]
dependencies = [
    "dagster>=1.13",
    "dagster-gcp>=0.29",
    "google-cloud-bigquery>=3.0",
]
```

`uv sync`.

---

## Step 2 — Shared partitions

```python
# src/neighborhood_intelligence_taxi/partitions.py
from dagster import MonthlyPartitionsDefinition

MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(start_date="2022-01-01")
```

Every asset in the graph uses this.

---

## Step 3 — Bronze — declare the public tables as external assets

Two `AssetSpec`s. `AssetSpec` is Dagster's way of declaring that
"this asset exists somewhere Dagster didn't create, and we want it
on the lineage graph." No function, no execution, no ingestion —
just a declaration.

```python
# src/neighborhood_intelligence_taxi/assets/bronze.py
from dagster import AssetSpec, AssetKey

source_yellow_trips = AssetSpec(
    key=AssetKey("source_yellow_trips"),
    group_name="bronze",
    kinds={"bigquery"},
    owners=["team:nit-data-platform"],
    description="NYC TLC Yellow Taxi trips (2022) — public dataset.",
    metadata={
        "bigquery/table":
            "bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022",
        "owner_team": "team:nit-data-platform",
        "tier": "tier_1",
        "domain": "trips",
    },
)

source_taxi_zone_lookup = AssetSpec(
    key=AssetKey("source_taxi_zone_lookup"),
    group_name="bronze",
    kinds={"bigquery"},
    owners=["team:nit-data-platform"],
    description="TLC Taxi Zone lookup — one row per LocationID.",
    metadata={
        "bigquery/table":
            "bigquery-public-data.new_york_taxi_trips.taxi_zone_geom",
        "owner_team": "team:nit-data-platform",
        "tier": "tier_1",
        "domain": "reference",
    },
)
```

Compare: the dbt+DCC version is a 20-line YAML file with a
component reference. The Python version is a 25-line dataclass — a
wash for two sources, but scales linearly with count.

---

## Step 4 — Silver — hand-written asset functions with real BigQuery SQL

Every `@asset` function is: partition context in, BigQuery `CREATE
OR REPLACE TABLE` out. You write the SQL. You handle the partition
window. You manage the schema.

```python
# src/neighborhood_intelligence_taxi/assets/silver.py
import os

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    asset,
)
from dagster_gcp import BigQueryResource

from neighborhood_intelligence_taxi.partitions import MONTHLY_PARTITIONS

SILVER_DATASET = "nit_silver"


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name="silver",
    kinds={"python", "bigquery"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey("source_yellow_trips")],
    description="Typed + null-filtered yellow taxi trips.",
)
def stg_yellow_trips(
    context: AssetExecutionContext, bigquery: BigQueryResource
) -> MaterializeResult:
    project = os.environ["NIT_BIGQUERY_PROJECT"]
    partition_start = context.partition_key
    partition_end = context.partition_time_window.end.strftime("%Y-%m-%d")

    with bigquery.get_client() as client:
        client.query(
            f"CREATE SCHEMA IF NOT EXISTS `{project}.{SILVER_DATASET}`"
        ).result()

        result = client.query(f"""
            CREATE OR REPLACE TABLE
              `{project}.{SILVER_DATASET}.stg_yellow_trips${partition_start.replace('-', '')[:6]}01`
            AS
            SELECT
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_location_id,
                dropoff_location_id,
                payment_type,
                fare_amount,
                tip_amount,
                tolls_amount,
                total_amount,
                DATE(pickup_datetime) AS pickup_date,
                EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour
            FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
            WHERE DATE(pickup_datetime) >= DATE('{partition_start}')
              AND DATE(pickup_datetime) <  DATE('{partition_end}')
              AND pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND pickup_location_id IS NOT NULL
              AND dropoff_location_id IS NOT NULL
        """).result()

    row_count = client.query(
        f"SELECT COUNT(*) AS n "
        f"FROM `{project}.{SILVER_DATASET}.stg_yellow_trips` "
        f"WHERE DATE(pickup_datetime) = DATE('{partition_start}')"
    ).result().to_dataframe()["n"].iloc[0]

    return MaterializeResult(
        metadata={"dagster/row_count": int(row_count)},
    )


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name="silver",
    kinds={"python", "bigquery"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey("source_taxi_zone_lookup")],
    description="Typed taxi zone reference table.",
)
def stg_taxi_zones(
    context: AssetExecutionContext, bigquery: BigQueryResource
) -> None:
    # Same shape as stg_yellow_trips: CREATE SCHEMA, CREATE OR REPLACE
    # TABLE, SELECT from bigquery-public-data.new_york_taxi_trips.taxi_zone_geom.
    ...
```

**Note the friction that dbt+DCC removed:**

- **Schema creation** — you write `CREATE SCHEMA IF NOT EXISTS`. dbt
  does this for you.
- **Partition window in SQL** — you interpolate `partition_key` +
  `partition_time_window.end` into a `WHERE` clause. dbt has
  Dagster's partition context injected for you.
- **Table naming** — you decide `stg_yellow_trips$202201` vs. one
  table with pickup_date partitioning. dbt has a `+materialized:
  table` config line and picks a shape.
- **Metadata** — you compute `row_count` yourself with a follow-up
  query. dbt+DCC surfaces it automatically.

Six models × ~50 lines each = ~300 lines of hand-written SQL.

---

## Step 5 — Gold — same shape, more of it

`fct_trips` joins the two silver tables. `agg_daily_zone`,
`agg_hourly_demand`, `agg_vendor_performance` each roll up
`fct_trips`. All four are @asset functions with real SQL bodies.

```python
# src/neighborhood_intelligence_taxi/assets/gold.py
@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name="gold",
    kinds={"python", "bigquery"},
    owners=["team:nit-data-platform"],
    deps=[AssetKey(["silver", "stg_yellow_trips"]), AssetKey(["silver", "stg_taxi_zones"])],
    description="Fan-in trip fact: enriched with pickup + dropoff zone metadata.",
)
def fct_trips(context, bigquery: BigQueryResource) -> None:
    # CREATE OR REPLACE TABLE nit_gold.fct_trips AS
    # SELECT t.*, pz.zone_name AS pickup_zone_name, ...
    # FROM nit_silver.stg_yellow_trips t
    # LEFT JOIN nit_silver.stg_taxi_zones pz ON t.pickup_location_id = pz.zone_id
    # LEFT JOIN nit_silver.stg_taxi_zones dz ON t.dropoff_location_id = dz.zone_id
    ...
```

Repeat this shape three more times for the aggregates. Each ~30–50
lines.

---

## Step 6 — Report — fan-out from gold

```python
# src/neighborhood_intelligence_taxi/assets/report.py
@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name="report",
    kinds={"python", "bigquery"},
    deps=[AssetKey(["gold", "agg_daily_zone"])],
    description="Daily summary report for the ops leadership team.",
)
def report_daily_summary(context, bigquery: BigQueryResource) -> None:
    ...


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name="report",
    kinds={"python", "bigquery"},
    deps=[AssetKey(["gold", "fct_trips"])],
    description="Top pickup zones by trip volume for the given month.",
)
def report_top_zones(context, bigquery: BigQueryResource) -> None:
    ...
```

Two more functions. Same shape.

---

## Step 7 — Reconciliation check

`@asset_check` is Dagster's native equivalent of dbt's generic
test. You write the check as a Python function that returns
`AssetCheckResult`.

```python
# src/neighborhood_intelligence_taxi/checks.py
import os

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetKey,
    asset_check,
)
from dagster_gcp import BigQueryResource


@asset_check(
    asset=AssetKey(["gold", "fct_trips"]),
    additional_deps=[AssetKey(["silver", "stg_yellow_trips"])],
    blocking=True,
    description=(
        "Row-count reconciliation: fct_trips must equal stg_yellow_trips. "
        "Catches the zone left-join silently dropping rows or a broken "
        "trip_id key."
    ),
)
def fct_trips_reconciliation(
    context: AssetCheckExecutionContext, bigquery: BigQueryResource
) -> AssetCheckResult:
    project = os.environ["NIT_BIGQUERY_PROJECT"]
    with bigquery.get_client() as client:
        rows = client.query(f"""
            SELECT
              (SELECT COUNT(*) FROM `{project}.nit_silver.stg_yellow_trips`) AS silver,
              (SELECT COUNT(*) FROM `{project}.nit_gold.fct_trips`)          AS gold
        """).result().to_dataframe()
    silver, gold = int(rows["silver"].iloc[0]), int(rows["gold"].iloc[0])
    return AssetCheckResult(
        passed=silver == gold,
        metadata={"silver_rows": silver, "gold_rows": gold, "diff": silver - gold},
        description=(
            f"fct_trips={gold} vs stg_yellow_trips={silver}"
            + ("" if silver == gold else f" (diff {silver - gold})")
        ),
    )
```

Not_null / unique on `fct_trips.trip_id` become similar
`@asset_check` functions — each ~15 lines of Python. In dbt they're
two lines of YAML per column.

---

## Step 8 — Schedule

```python
# src/neighborhood_intelligence_taxi/schedules.py
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from neighborhood_intelligence_taxi.partitions import MONTHLY_PARTITIONS

monthly_full_pipeline_job = define_asset_job(
    name="monthly_full_pipeline_job",
    selection=AssetSelection.groups("silver", "gold", "report"),
    partitions_def=MONTHLY_PARTITIONS,
)

monthly_full_pipeline_schedule = build_schedule_from_partitioned_job(
    job=monthly_full_pipeline_job,
    default_status=DefaultScheduleStatus.STOPPED,
)
```

Same fire-cadence-derived-from-partitions_def behavior as the DCC
`cron_schedule` component, in Dagster-native form.

---

## Step 9 — Wire it all up

```python
# src/neighborhood_intelligence_taxi/definitions.py
import os

from dagster import Definitions
from dagster_gcp import BigQueryResource

from neighborhood_intelligence_taxi.assets.bronze import (
    source_yellow_trips,
    source_taxi_zone_lookup,
)
from neighborhood_intelligence_taxi.assets.silver import (
    stg_yellow_trips, stg_taxi_zones,
)
from neighborhood_intelligence_taxi.assets.gold import (
    fct_trips, agg_daily_zone, agg_hourly_demand, agg_vendor_performance,
)
from neighborhood_intelligence_taxi.assets.report import (
    report_daily_summary, report_top_zones,
)
from neighborhood_intelligence_taxi.checks import fct_trips_reconciliation
from neighborhood_intelligence_taxi.schedules import monthly_full_pipeline_schedule


defs = Definitions(
    assets=[
        source_yellow_trips, source_taxi_zone_lookup,
        stg_yellow_trips, stg_taxi_zones,
        fct_trips, agg_daily_zone, agg_hourly_demand, agg_vendor_performance,
        report_daily_summary, report_top_zones,
    ],
    asset_checks=[fct_trips_reconciliation],
    schedules=[monthly_full_pipeline_schedule],
    resources={
        "bigquery": BigQueryResource(project=os.environ["NIT_BIGQUERY_PROJECT"]),
    },
)
```

---

## Step 10 — Load in the UI

```bash
export NIT_BIGQUERY_PROJECT=your-gcp-project
export NIT_BIGQUERY_DATASET=nit_taxi
uv run dg dev
```

Same graph as [WALKTHROUGH.md](./WALKTHROUGH.md). Bronze
external assets flow into silver, gold, report. Every asset badged
`python` + `bigquery`. Three groups. One monthly schedule. Two
column checks + one reconciliation check.

---

## What this walkthrough is really showing

Line-count contrast between the two paths:

| Layer | dbt + DCC (this repo) | Pure Python |
|---|---|---|
| Bronze (2 external asset declarations) | ~30 lines YAML | ~25 lines Python |
| Transformation wiring (silver + gold + report) | 1 YAML file + 1 template_vars.py, ~50 lines | 3 files × ~200 lines Python each = ~600 lines |
| Schedule | 1 YAML file, ~30 lines | ~15 lines Python |
| Reconciliation check | 1 SQL macro + 5 lines schema.yml | ~30 lines Python |
| Column checks (not_null, unique) | 2 lines YAML per test | ~15 lines Python per test |
| **Total** | **~150 lines** | **~750 lines** |

Same graph. Same behavior. Fifth the code. That's the trade you're
evaluating when you consider adopting dbt and the DCC — you're
choosing where you want to write SQL vs. Python, and whether you
want the framework to handle schema creation, partition windowing,
and check-to-model wiring for you.

Neither path is wrong. Pick based on what your team already knows
and what conventions you want to enforce across many pipelines.
