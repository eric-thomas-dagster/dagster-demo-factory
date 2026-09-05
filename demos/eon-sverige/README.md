# E.ON Sverige — Dagster demo

## "Grid at Scale"

## The pitch

E.ON Energidistribution is mid-way through its largest-ever Swedish grid
investment (23-27 billion SEK, 2024-2027) and a nationwide smart-meter
replacement bringing ~1M new NB-IoT-connected meters online. At the same
time, EU Implementing Regulation 2026/855 (in force since April 2026)
requires transparent, auditable data-access procedures for
customer-switching data. This demo shows a grid-and-metering data platform
that absorbs the step-change in telemetry volume with per-region
completeness and range checks, and produces the auditable trail 2026/855
demands from the same asset-level lineage and metadata — not a separate
compliance system.

**Confidence on every technical specific here is low.** No AE discovery
notes exist for E.ON Sverige — this build (and the brief it came from,
`briefs/2026-09-17-eon-sverige.md`) is built entirely from public
investment/regulatory signal and one internal sizing document. Say so
plainly in the room. See **Assumptions** below.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required.** This is a graph-first demo (see below) — there is no database,
no credentials, and no `demo_mode` toggle to configure. Every asset body is
a no-op; the graph, checks, freshness policy, automation, and schedule are
all real.

## Fidelity: graph-first

Per the brief, no named integration exists to build for real and confidence
on every specific is low, so every asset body is a trivial no-op
(`GraphFirstAssetsComponent`, reused verbatim from the City of Detroit DWSD
build — see below). There is no synthetic data generator and no mocked I/O,
because there is no live-system integration to fake in the first place.
Everything materializes instantly and is always green.

This also means there's nothing to "go live" against for this specific
build. The natural next step, if this call surfaces a real orchestrator,
warehouse, or metering system name, is to replace the relevant
`GraphFirstAssetsComponent` entries with real components against E.ON's
actual systems, following the same demo-mode pattern
(`templates/demo_mode_pattern.py`) every data-backed demo in this repo uses.

## The asset graph

Eight assets across four groups, matching the brief's build directive
exactly (the brief's own "~10-12 assets" sizing note doesn't match its
explicit 8-item enumeration — this build follows the enumerated list, same
resolution as the Trafigura build's identical mismatch):

- **`metering_ingestion`** (2 assets) — `raw_meter_reads` (partitioned by
  date x grid bidding zone — Sweden's four national SE1-SE4 electricity
  zones, a public convention, not an invented E.ON-specific system name) and
  `raw_grid_load_telemetry` (daily-partitioned).
- **`grid_quality`** (2 assets) — `validated_meter_reads` (downstream of
  `raw_meter_reads`, same multi-partition) and `grid_load_hourly`
  (downstream of `raw_grid_load_telemetry`, daily-partitioned, carries the
  demo's one freshness policy).
- **`customer_switching_compliance`** (2 assets) — `customer_switching_extract`
  (downstream of `validated_meter_reads`) and `switching_data_audit_log`
  (downstream of the extract, gated on its blocking check via
  `all_deps_blocking_checks_passed`).
- **`grid_reporting`** (2 assets) — `daily_grid_health_summary` and
  `regional_meter_coverage_report`.

No `kinds` badge on any asset — the brief is explicit that no stack layer is
confirmed for E.ON (orchestrator, warehouse, transformation, ingestion, BI,
and cloud are all "Unknown" in its Current Stack table), and guessing a
vendor icon risks a confident-wrong answer in the room. This mirrors the
same choice made on the City of Detroit DWSD warehouse/reporting layers.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `meter_reads_completeness_check` | `raw_meter_reads` | **Blocking** | An expected meter ID missing a reading for its zone/day — the "step-change in telemetry volume" pain as the meter rollout lands. |
| `grid_load_range_check` | `raw_grid_load_telemetry` | **Blocking** | A physically implausible load value before it reaches the hourly rollup. |
| `switching_extract_audit_completeness_check` | `customer_switching_extract` | **Blocking** | A missing required audit field on the customer-switching extract — the EU 2026/855 pain, directly. Made blocking rather than the brief's "warning-only acceptable" option, since the brief itself prefers blocking and two other blocking checks already exist. |

All three always pass (graph-first, no planted anomaly) and report, in
their metadata, the exact threshold or required-field set they'd enforce
against real data in production.

## Freshness, automation, and schedule

- **Freshness policy** (the brief's one): `grid_load_hourly` — fail at 6h,
  warn at 4h. Chosen over the meter-read or compliance assets because grid
  load is the one E.ON's grid-operations team would page someone about.
- **Automation conditions**: `AutomationCondition.eager()` on every
  `grid_quality`, `customer_switching_compliance`, and `grid_reporting`
  asset except `switching_data_audit_log`, which additionally gates on
  `all_deps_blocking_checks_passed()` — it never rebuilds off a
  customer-switching extract whose own audit-completeness check has failed.
- **Schedule**: `eon_daily_telemetry_ingestion_schedule` materializes
  `raw_grid_load_telemetry` at 03:00 CET (Europe/Stockholm) — two hours
  ahead of the 05:00 CET SLA on its metadata. Scoped to this one asset
  rather than also `raw_meter_reads`, which is multi-partitioned (date x
  zone) and would need a partition mapping this graph-first build has no
  reason to add.
- **Retry policies**: none. Nothing here is a real network call, so a
  decorative retry would invite a question we'd lose.

## Three buckets

1. **Implemented in code**: the eight-asset graph, its three checks, one
   freshness policy, automation conditions, and the daily telemetry
   schedule.
2. **Handled by Dagster+, demonstrated not built**: alerting on check
   failures (Slack/email/Teams), restart-from-failure, run history, lineage
   visualization, asset health.
3. **Conversation only, nothing built**: any real integration with
   Landis+Gyr's Gridstream Connect platform, any real SAP IS-U or
   grid-operations system, and the specifics of how EU 2026/855 compliance
   reporting would actually be delivered to a regulator — none of E.ON's
   real systems are confirmed, so these are discussion points, not code.

## `defs/` file count

**4 YAML, 2 Python** (plus a boilerplate empty `defs/__init__.py`):

- `defs/metering_ingestion/defs.yaml`, `defs/grid_quality/defs.yaml`,
  `defs/customer_switching_compliance/defs.yaml`,
  `defs/grid_reporting/defs.yaml` — all eight assets, entirely declarative,
  one shared component (`GraphFirstAssetsComponent`) instantiated four
  times.
- `defs/checks/checks.py` — **justified**: asset-check assertion logic is
  business logic; no registry component covers a declarative
  completeness/range/audit check (this gap was already searched and
  recorded on the City of Detroit DWSD build — "asset check declarative
  yaml", "row count completeness check", 2026-08-28 — not re-searched here
  since it's the identical need). Three checks in one file rather than
  three, since none share code.
- `defs/automation/daily_telemetry_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't express
  a specific local hour alongside a `partitions_def` (confirmed by reading
  its source on the DWSD build); the one-function native
  `build_schedule_from_partitioned_job` call is used directly instead.

`components/graph_first_assets.py` is the one custom component (see below)
— components are expected to hold Python; only `defs/` is measured for the
YAML-first ratio.

## Custom component: `GraphFirstAssetsComponent`

Reused verbatim from `demos/detroit-dwsd/src/detroit_dwsd/components/graph_first_assets.py`,
including its shared `daily_partitions` and `recompute_after_upstream_checks_pass`
template vars, with one addition: a `meter_reads_partitions` template var
(`MultiPartitionsDefinition` of date x grid zone) for the multi-region meter
rollout this brief specifically calls for. No native or registry component
declares a list of no-op assets from YAML — that's a generic authoring
primitive, not an integration domain, so the subclass rung of the escalation
ladder doesn't apply (nothing to subclass). Full search record:
`component-feedback/2026-08-28-graph-first-assets.md`.

## Which parts run where

Everything in this demo is an in-memory no-op plus Dagster's own metadata —
there's no local file or database whose state needs to survive between
runs. So, unlike data-backed demos in this repo, **there is no
Serverless-ephemeral-storage caveat here**: the same materialize sequence
works identically in `dg dev` and in the deployed Dagster+ code location.
`dg dev` is still the natural place to click through the graph and the
metadata panels on a shared screen.

## Assumptions

Everything below was inferred, not confirmed — there are no AE discovery
notes for this prospect (see the brief, `briefs/2026-09-17-eon-sverige.md`,
for full sourcing):

- **That the use case is grid/metering telemetry ingestion plus
  customer-switching compliance.** Inferred from E.ON's public grid
  investment and smart-meter rollout announcements plus EU 2026/855, not a
  stated requirement.
- **The four SE1-SE4 grid-zone partition dimension.** A real, national
  Swedish electricity-market convention (Svenska kraftnät's bidding zones),
  used here as a plausible generic region dimension for the meter rollout —
  not a claim about which zones E.ON Energidistribution specifically
  operates in or how its internal systems are regionalized.
- **The asset count (8, not the brief's "~10-12" note).** Built the brief's
  own explicit enumeration rather than padding to the sizing note's range —
  the same resolution used on the Trafigura build for an identical
  sizing-note/enumeration mismatch.
- **Daily partition cadence and the 05:00 CET SLA.** No real SLA numbers
  exist publicly; daily is the brief's stated safe default, and 05:00 CET is
  this build's own plausible-but-invented deadline for the schedule to
  target.
- **No `kinds` badge anywhere.** Every stack layer is "Unknown" per the
  brief's own Current Stack table; badging any asset would be a guess the
  brief explicitly warns against.
- **No dbt, no Snowflake, no Fivetran, no Databricks.** None of these are
  confirmed for E.ON — only patterns from unrelated Swedish job postings,
  which the brief explicitly says not to treat as evidence.
