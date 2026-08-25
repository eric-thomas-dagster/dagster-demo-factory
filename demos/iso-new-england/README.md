# ISO New England — Dagster demo

Built for the 2026-08-26 technical deep-dive. **Read this before touching
anything on the shared screen.**

## Demo thesis

ISO-NE is already running Dagster OSS against dbt/Oracle/Postgres and
learning sensors on their own. This demo isn't selling orchestration — it's
selling the platform layer above the orchestration they already trust:
catalog, lineage, freshness, alerting, and governance, framed against a
highly regulated, NERC-CIP-governed organization that will need to prove all
of this to auditors eventually anyway.

## Getting started (zero setup)

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open <http://localhost:3000>. No environment variables, no credentials, no
manual file creation — the demo warehouse (`demo_data/demo.duckdb`) and the
mock advisory-feed state are created and seeded automatically on first read.

Materialize everything from the UI (select all assets → Materialize), or
headlessly via the validation harness:

```bash
python validate_e2e.py
```

## The asset graph

**Oracle/legacy source → Postgres landing → dbt staging → dbt intermediate
→ dbt marts**, one daily partition per asset (open-ended, extends through
"today"):

- `raw/legacy_oracle_extract` (kind `oracle`) — the legacy Oracle
  interval-telemetry batch, on a **fixed 2am ET schedule**. This is today's
  actual ISO-NE pattern: a dumb fixed interval, whether or not there's new
  work to do.
- `raw/external_feed_raw` (kind `oracle`) — the external operations-advisory
  feed, triggered by `external_feed_arrival_sensor` **the moment a new day's
  batch is published** — the direct contrast to the schedule above.
- `staged/staged_readings`, `staged/staged_reference` (kind `postgres`) —
  the Postgres landing zone. `staged_readings` carries the **blocking**
  `staged_readings_completeness` check: dbt never reads an incomplete batch.
- `staging/stg_readings`, `staging/stg_reference` → `intermediate/int_readings_validated`,
  `intermediate/int_daily_rollup` → `marts/mart_daily_operations_summary`,
  `marts/mart_source_reliability`, `marts/mart_capacity_trend`,
  `marts/platform_status_report` — a real dbt Core project against DuckDB,
  badged `postgres` (their real target warehouse, not a disguise).
- `marts/platform_status_report` is the money-shot asset: a per-day status
  view (`nominal` / `advisory_active` / `degraded_quality`) with a freshness
  policy — Andrew's "communication mechanism for users" ask, made concrete.

## Run-of-show

1. **Open the asset graph.** Everything is green. Point out the group
   structure (ingestion → landing → staging → intermediate → marts →
   reporting) and the `oracle`/`postgres`/`dbt` kind badges.
2. **Click into `legacy_oracle_extract`.** Show the schedule
   (`legacy_oracle_extract_schedule`, 2am ET, fixed interval) — "this is
   your current pattern."
3. **Click into `external_feed_raw`.** Show `external_feed_arrival_sensor`
   instead — toggle it on a few minutes before the meeting so its first
   tick (which back-fills the whole historical window in one pass) settles
   before you're on screen.
4. **The live money shot:** run `make simulate-event` (or
   `python -m iso_new_england.demo_data.simulate_new_advisory`) from a
   terminal. Within 30s the sensor fires a run for *only* today's partition
   — not a full-window backfill — and downstream dbt assets recompute
   automatically via `AutomationCondition.eager()`.
5. **Click into `staged_readings_completeness`.** Show it's blocking, passing,
   and explain what it protects against — a partial batch never reaches dbt.
   (Verified during the build to actually fail on a truncated batch — see
   `validate_e2e.py` step 3 — but the shipped demo stays green throughout.)
6. **Click into `platform_status_report`.** Show the freshness policy and the
   `platform_status` column — "this is the status page Andrew asked for."
7. **Pivot to Dagster+**: catalog/lineage view, RBAC settings. *"This is what
   you get on top of the orchestration you already trust — and it's the
   head start on the NERC CIP governance review your security team is going
   to run eventually anyway."*

## What's mocked vs. real

| Component | Demo mode | Real mode |
|---|---|---|
| `legacy_oracle_extract` | Deterministic synthetic readings | Queries Oracle via `oracle_resource` (community registry, subclassed as `DemoOracleResource`) |
| `external_feed_raw` | Deterministic synthetic advisories, gated on mock feed-arrival state | Real vendor feed client (not implemented — see code comment) |
| Postgres landing (`staged_readings`/`staged_reference`) | Writes to local DuckDB via `DemoPostgresResource` | Writes to real Postgres (`postgres_resource`, community registry, subclassed) |
| dbt staging/intermediate/marts | **Real dbt Core**, against DuckDB | Same dbt project, `profiles.yml` target `live` → real Postgres |

**To flip to live mode:** set `ISO_NE_DBT_TARGET=live` plus the Postgres/Oracle
env vars referenced in `profiles.yml` and the two `defs.yaml` files under
`defs/ingestion/`, and set `demo_mode: false` on each component's YAML
attributes. Asset keys, partitions, checks, and lineage are byte-identical
either way — only the network boundary changes.

Real-mode Postgres/Oracle connections need `psycopg2-binary` / `oracledb`,
which aren't installed in demo mode (`uv add psycopg2-binary oracledb`
before flipping `demo_mode: false`).

## Local vs. Dagster+

- **Give the live demo locally with `dg dev`.** The sensor, the schedule,
  and the `make simulate-event` money shot all depend on local process state
  and a local DuckDB file — Dagster+ Serverless containers are ephemeral, so
  a multi-run sequence like this only works locally.
- **The Dagster+ deployment is the proof point that the project is real** —
  it loads, the graph renders, the code location is genuine. Don't try to
  click through the sensor/recovery sequence there.

## The planted-failure story (build-time only)

Per the brief, this meeting calls for a green walkthrough throughout — no
staged failure demonstration. `staged_readings_completeness` is a real
blocking check with real logic (not a decorative always-pass), and
`validate_e2e.py` proves it during the build by truncating one partition's
raw batch, confirming the check fails and blocks downstream compute, then
restoring the source and rematerializing — a plain rematerialize, no heal
step, per the idempotency rule. Nothing from that probe is left in the
committed demo state.

## Resetting the demo

```bash
make reset-demo
```

Deletes the local DuckDB warehouse and the mock advisory-feed state — an
operation on the mock source, never on Dagster. The next run re-seeds the
historical window automatically.

## Skipped: retry policies

No source in this demo is modeled as flaky, so no `RetryPolicy` was added —
a decorative retry on a deterministic synthetic source invites a "why does
this need a retry?" question we'd rather not answer live.
