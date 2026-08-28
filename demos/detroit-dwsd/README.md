# City of Detroit — DWSD — Dagster demo

## "One Warehouse, Every Pipe"

## The pitch

DWSD's own job posting for "Applications Analyst 2 (Integrations)" asks for
someone to hand-code T-SQL, PL/SQL, Java, and Python integrations moving data
from "legacy, web, cloud, and purchased package environments" into DWSD's
data warehouse, including "high frequency data loads." No orchestrator,
lineage tool, or checks layer is named anywhere in that posting. Read
plainly, that's a data engineer being hired to hand-maintain point-to-point
integration sprawl.

This demo shows what governing that sprawl would look like: four
heterogeneous source systems (badged to match DWSD's own named stack —
`sqlserver` for the T-SQL-flavored systems, `oracle` for the PL/SQL-flavored
ones), one lineage graph, real freshness policies and blocking checks, and
declarative automation that rebuilds the warehouse without a cron job anyone
has to remember exists. The concrete hook: when a resident's water bill is
wrong or an EPA compliance report is late, "which job touched this data and
did it pass its checks" is a question this graph can answer that DWSD's
current stack (per the posting) cannot.

This is a 30-minute first intro call with one hands-on attendee (Anthony
Urbina) and no confirmed AE discovery notes — see **Assumptions** below for
everything inferred rather than confirmed.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required.** This is a graph-first demo (see below) — there is no database,
no credentials, and no demo-mode I/O apparatus to configure. Every asset
body is a no-op; the graph, checks, freshness policies, and automation are
all real.

## Fidelity: graph-first

Per the brief, this demo is about lineage, checks, freshness, and
declarative automation across a heterogeneous source landscape — not about
a specific row count on screen. So every asset body is a trivial no-op
(`GraphFirstAssetsComponent`, see below); there is no synthetic data
generator, no mocked I/O, and no `demo_mode` toggle to flip, because there is
no live-system integration to fake in the first place. Everything
materializes instantly and is always green, because a no-op asset can't
fail.

This also means there's nothing to "go live" against for this specific
build — the natural next step, if this call surfaces real system names, is
to replace the relevant `GraphFirstAssetsComponent` entries with real
`dagster-dbt` / `mssql_resource` / `oracle_resource` components against
DWSD's actual systems, following the same demo-mode pattern
(`templates/demo_mode_pattern.py`) every data-backed demo in this repo uses.

## The asset graph

Eleven assets across three groups, one lineage graph:

- **`dwsd_ingestion`** (4 assets) — `billing_system_extract` and
  `work_order_extract` (badged `sqlserver`), `meter_reading_extract` and
  `water_quality_lab_extract` (badged `oracle`, both daily-partitioned —
  these are the two domains the brief names as "high frequency" /
  regulatory-cadence).
- **`dwsd_warehouse`** (5 assets) — `dim_customer_account`,
  `fact_meter_reads`, `fact_billing_usage`, `water_quality_compliance_daily`,
  `work_order_status`. Each depends on its ingestion source(s); the two
  daily chains (`meter_reading_extract` → `fact_meter_reads`;
  `water_quality_lab_extract` → `water_quality_compliance_daily`) stay
  partitioned end to end.
- **`dwsd_reporting`** (2 assets) — `billing_accuracy_report` (the
  reconciliation artifact) and `compliance_reporting_extract` (the artifact
  that would go to a regulator, gated on `water_quality_compliance_daily`'s
  blocking check via `all_deps_blocking_checks_passed`).

No `kinds` badge on the warehouse/reporting layers — DWSD's actual warehouse
product isn't confirmed publicly, and the brief is explicit that guessing a
specific vendor is worse than leaving it blank.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `meter_reading_extract_completeness` | `meter_reading_extract` | **Blocking** | An incomplete day of AMI reads before it reaches billing. |
| `water_quality_compliance_daily_completeness` | `water_quality_compliance_daily` | **Blocking** | A missing required lab reading before it reaches the compliance extract. |
| `billing_accuracy_report_reconciliation` | `billing_accuracy_report` | Warning | Billed usage diverging from raw meter delta. |

All three always pass (graph-first, no planted anomaly) and report, in their
metadata, the exact threshold or required-field set they'd enforce against
real data in production — the talk track is what each catches when it's
wired to a live feed, not a live number today.

## Freshness, automation, and schedule

- **Freshness policies**: `water_quality_compliance_daily` (fail at 26h,
  warn at 20h) and `billing_accuracy_report` (fail at 30h, warn at 24h) —
  the two assets DWSD would page someone about.
- **Automation conditions**: `AutomationCondition.eager()` on every
  warehouse/reporting asset except `compliance_reporting_extract`, which
  additionally gates on `all_deps_blocking_checks_passed()` — it never
  rebuilds off an incomplete compliance rollup.
- **Schedule**: `dwsd_daily_ingestion_schedule` materializes the two
  partitioned ingestion sources at 4:00 AM (partitions_def timezone:
  America/New_York) — two hours ahead of the 6:00 AM SLA on their metadata.
- **Retry policies**: none. Per house rules, a decorative retry on a
  deterministic synthetic/no-op source invites a question we'd lose; nothing
  here is genuinely flaky.

## Three buckets

1. **Implemented in code**: the eleven-asset graph, its three checks, two
   freshness policies, automation conditions, and the daily schedule.
2. **Handled by Dagster+, demonstrated not built**: alerting on check
   failures (Slack/email/Teams), restart-from-failure, run history, lineage
   visualization, asset health. None of this is hand-rolled here.
3. **Conversation only, nothing built**: the citywide angle (DoIT's existing
   Airflow+dbt pipeline for the open-data portal, the Health Department's
   from-scratch warehouse build, and the fact that three City of Detroit
   departments are independently solving similar problems under one Chief
   Data Officer) — worth raising verbally if the call goes there, not part
   of this graph.

## `defs/` file count

**3 YAML, 2 Python** (plus a boilerplate empty `defs/__init__.py`):

- `defs/dwsd_ingestion/defs.yaml`, `defs/dwsd_warehouse/defs.yaml`,
  `defs/dwsd_reporting/defs.yaml` — all eleven assets, entirely declarative,
  one shared component (`GraphFirstAssetsComponent`) instantiated three
  times.
- `defs/checks/checks.py` — **justified**: asset-check assertion logic is
  business logic; no registry component covers a declarative completeness/
  reconciliation check (searched "asset check declarative yaml", "row count
  completeness check", 2026-08-28). Three checks combined into one file
  rather than three, since none share code and a `defs/` tree with a
  three-line file per check is worse, not more YAML-first.
- `defs/automation/daily_ingestion_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't express
  a specific local hour (confirmed by reading its source; passing
  `cron_expression`/`execution_timezone` alongside `partition_type` raises a
  `CheckError`), so the one-function native `build_schedule_from_partitioned_job`
  call is used directly instead.

`components/graph_first_assets.py` is the one custom component (see below)
— components are expected to hold Python; only `defs/` is measured for the
YAML-first ratio.

## Custom component: `GraphFirstAssetsComponent`

No native or registry component declares a list of no-op assets from YAML —
that's a generic authoring primitive, not an integration domain, so the
subclass rung of the escalation ladder doesn't apply (nothing to subclass).
Full search record and suggested registry addition:
`component-feedback/2026-08-28-graph-first-assets.md`.

## Which parts run where

Everything in this demo is either an in-memory no-op or Dagster's own
metadata — there's no local file or database whose state needs to survive
between runs. So, unlike data-backed demos in this repo, **there is no
Serverless-ephemeral-storage caveat here**: the same materialize sequence
works identically in `dg dev` and in the deployed Dagster+ code location.
`dg dev` is still the natural place to click through the graph and the
metadata panels on a shared screen.

## Assumptions

Everything below was inferred, not confirmed — there are no AE discovery
notes for this prospect, only a 14-day calendar scan and public research
(see the brief, `briefs/2026-08-28-detroit-dwsd.md`, for full sourcing):

- **Which source system is `sqlserver` vs `oracle`.** The brief confirms
  T-SQL and PL/SQL as named DWSD skills but not which system runs which.
  This build assigned `sqlserver` to the billing/CIS and work-order/CMMS-
  style systems and `oracle` to the AMI meter and water-quality-lab systems
  — a plausible but unconfirmed split.
- **That DWSD's data domains are billing, metering, work orders, and
  water-quality lab data.** Inferred from the posting's function and
  industry norms for a US water utility (Safe Drinking Water Act
  compliance), not confirmed for DWSD specifically.
- **Daily partition cadence and the 6:00 AM SLA.** No real SLA numbers
  exist publicly; daily is the brief's stated safe default, and 6:00 AM is
  this build's own plausible-but-invented deadline for the schedule to
  target.
- **No dbt, no Airflow.** The city's own `airflow-dbt-python` GitHub fork is
  evidence for a *different* department (DoIT's open-data portal), not
  DWSD — deliberately not built here per the brief's explicit instruction.
