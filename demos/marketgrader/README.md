# MarketGrader.com — "One Index, End to End"

A Dagster demo for MarketGrader.com's 2026-09-25 technical deep-dive. Built
from `briefs/2026-09-25-marketgrader.md` (rebuilt from an AE discovery-call
doc; see `REBUILD NOTICE` at the top of that brief).

**Demo thesis:** MarketGrader is replacing a serial SQL Server/SSIS-via-SQL-
Agent batch chain — silent failures, tribal knowledge, manual checks — with
asset-based pipelines on a target Iceberg/Polaris lake. The near-term POC
scope is **one index, end-to-end**: migrate the Barron's 400 index fully,
prove it, while every other index keeps running on the same SSIS packages
as today. The money shot is one lineage graph spanning both the legacy
estate and the new lakehouse pipeline.

**Revised live, 2026-09-24 (Eric, reviewing the build):** the AE notes'
"MS SQL Server + SSIS run serially via SQL Agent" describes a real
dependency, and Eric asked directly whether Dagster could orchestrate the
legacy SSIS packages rather than only watch them. It can — the real
`ssis_workspace` component supports genuine triggering
(`action: execute`), not just observation, so **Dagster now takes over
scheduling for the legacy SSIS chain too, replacing SQL Agent as the
trigger, while the SSIS packages themselves are kept exactly as they are.**
The polling observation sensor stays on regardless, so a package someone
still kicks off by hand outside Dagster is still caught. This is Pattern 2
("Dagster calls legacy") layered with the observation half of Pattern 4 —
see `integration_pattern: "dagster_calls_legacy"` on the legacy assets.

## Getting started

```bash
git clone <repo> && cd demos/marketgrader
uv sync
source .venv/bin/activate
dg dev
```

No environment variables, no credentials, no manual file creation. The
target Iceberg lake is a real, local PyIceberg SQL (SQLite) catalog created
on first materialize under `src/marketgrader/demo_data/iceberg_warehouse/`
(gitignored) — genuine Iceberg table format, no external Polaris/REST
catalog required. The legacy SQL Agent/SSIS estate is fully simulated (see
Demo mode below).

## The asset graph

- **`legacy_estate`** (2 assets, unpartitioned): `legacy_sql_agent_price_ingest`,
  `legacy_sql_agent_index_calc` (`deps: [legacy_sql_agent_price_ingest]` —
  the AE notes' "runs serially" is a real dependency, both packages execute
  together in that order). Dagster now triggers + polls both SSIS packages
  directly (`action: execute`) — genuine orchestration through the real
  community-registry `ssis_workspace` component, replacing SQL Agent as the
  scheduler for every index not yet migrated, while the SSIS packages
  themselves are untouched. Its polling sensor is also on, so anything
  triggered outside Dagster still shows up.
- **`lakehouse_ingestion`** (2 assets, daily-partitioned): `raw_price_feed`,
  `raw_corporate_actions`. Real `pyarrow.Table` writes through the real
  community-registry `iceberg_io_manager` component, into the local
  PyIceberg catalog. Iceberg-badged (`kinds: ["iceberg"]` — verified in the
  installed `dagster-webserver` bundle at build time, not guessed).
- **`index_calculation`** (1 asset, daily-partitioned): `barrons_400_constituents`
  — the one index migrated end-to-end for this POC. Downstream of both raw
  feeds; recomputes the moment they land (eager automation), gated on their
  blocking arrival/completeness checks. Graph-first (Axis 2 stubbed) — no
  real scoring math, just the lineage/checks/freshness/automation story.
- **`customer_egress`** (1 asset, daily-partitioned): `licensee_distribution_extract`
  — the feed to fund sponsors/institutions licensing the index.
- **`reporting`** (1 asset, daily-partitioned): `daily_coverage_summary` — a
  rollup an ops/research persona would check.

7 assets total, matching the brief's own explicit enumeration (its prose
also says "~8 assets" in one place — this build follows the itemized list,
the same resolution used for several earlier builds' similar mismatches).

## Three buckets

**Implemented in code (this repo):**
- The 7 assets above.
- 5 asset checks: 2 blocking (`raw_price_feed_arrival`,
  `raw_corporate_actions_arrival`), 1 warning
  (`barrons_400_constituents_membership_bounds` — the brief's 3rd named
  check), plus 2 native `ssis_freshness_lag` checks from the SSIS workspace
  component itself (bonus — free from the registry component, not one of
  the brief's 3 named checks).
- 1 freshness policy (`barrons_400_constituents`, 4h fail / 2h warn — this
  build's own assumption; no SLA is named in the brief).
- Eager automation on the calculation → egress → reporting chain, gated on
  the two blocking raw-feed checks for `barrons_400_constituents`.
- Dagster orchestrates the legacy SSIS chain directly (`action: execute`)
  on its own 4:00 AM ET schedule (`marketgrader_legacy_orchestration_schedule`),
  replacing SQL Agent's own trigger — plus a polling observation sensor
  (`ssis_workspace_observation_sensor`, off by default on the registry
  component, turned on here) that still catches anything triggered outside
  Dagster.
- One 5:00 AM ET ingestion schedule (`marketgrader_daily_ingestion_schedule`)
  for the new lakehouse feeds — this build's own assumption of "well ahead
  of the 9:30 AM ET market open," not stated in the brief. Independent of
  the legacy chain's own schedule (different indices, not chained).
- Narrative metadata: `integration_pattern: "dagster_calls_legacy"`,
  `legacy_system_boundary: "ssis_packages_retained_dagster_orchestrated"`
  on the legacy assets; `business_impact`, `severity` on the assets that
  matter to the room.

**Handled by Dagster+ (demonstrate, don't build):**
- Alerting on check/freshness failure — point at the alert-policy UI live.
  The AE notes' own suggested flow says "fires an alert"; this repo builds
  the checks the alert would fire on, never the alert itself.
- Restart-from-failure — directly answers Jim's discovery question #3 ("who
  reruns a failed calc if the in-house expert is out").
- RBAC and audit trail — Carlos/finance's stated interest, and the real
  answer to the Pro-vs-Starter tier question the AE notes raise.
- Branch deployments, asset catalog, run history/duration trends.
- Hybrid deployment topology for IBM Cloud — point at the concept only;
  no attempt to actually deploy against IBM Cloud for this demo.

**Conversation only (mention, build nothing):**
- The specific price/corporate-action data vendor (unnamed in the brief).
- Real IBM Cloud hybrid-agent wiring.
- Infosec kickoff, POC scope/timeline negotiation.
- The Prefect merger question — Eric's to answer personally before the
  room, per the brief's explicit instruction. Not addressed anywhere in
  this build.

## Correction this build makes (house rules override the brief)

The AE notes' own suggested demo flow (step 3) is a **live planted
failure**: "a missing price file fails an asset check and fires an alert.
Then rerun only the affected downstream assets." House rules are explicit
that demos never plant a failure, with no exception a brief can grant — and
the brief itself already flags this override rather than silently dropping
it (see its "Demo mode" and "Explicitly out of scope" sections). This build
honors that: the exact same checks, alerting-policy pointer, and
restart-from-failure capability the AE notes describe are built and real,
but delivered as a **talk track against a green graph**, never staged live.
Flagged again in the deployment notification so it doesn't surprise Eric
mid-demo-prep.

## Demo mode

| Layer | Demo mode | Real mode (`demo_mode: false`) |
|---|---|---|
| Legacy SQL Agent/SSIS estate | `FakeSsisEngine` (`demo_data/legacy_ssis.py`) — no SQL Server credentials required. Same discovery/sensor/check/execute SQL (`create_execution`/`start_execution`/poll) runs unmodified against it. | Point `workspace.server/user/password` (real `SsisWorkspaceComponent` fields, unchanged) at MarketGrader's real SSISDB. |
| Target Iceberg lake | Local PyIceberg SQL (SQLite) catalog, zero setup, genuinely Iceberg-format on disk. | Set `catalog_uri`/`warehouse` (real `IcebergIOManagerComponent` fields, unchanged) to MarketGrader's real Polaris/REST catalog. |
| Price/corporate-action feeds | Deterministic synthetic generators (`demo_data/lakehouse.py`), seeded per-partition-date — same universe, same counts, every run. | No real-mode path exists — vendor unnamed in the brief; wire in a real ingestion source when one is confirmed. |

**Everything in this demo runs locally via `dg dev`.** Dagster+ Serverless
storage is ephemeral (fresh container per run) — the local PyIceberg catalog
and SSISDB mock would not persist across Serverless runs. Give the live,
interactive walkthrough locally; treat the Dagster+ deployment as proof the
project loads and the graph renders, not as where you click through the
multi-run recovery story.

## `defs/` file count

**6 YAML, 9 Python** in `defs/`. All 7 assets are YAML-instantiated
(`legacy_estate`, `lakehouse_ingestion`, `index_calculation`,
`customer_egress`, `reporting`, `resources` — one `defs.yaml` each). The 9
`.py` files, each justified:

- `checks/raw_price_feed_arrival.py`, `checks/raw_corporate_actions_arrival.py`,
  `checks/barrons_400_constituents_membership_bounds.py` — asset-check
  assertion logic is business logic; no registry component covers a
  declarative completeness/bounds check against an arbitrary Iceberg table.
- `automation/daily_ingestion_schedule.py` — `dg.build_schedule_from_partitioned_job`
  needs a specific local hour alongside a `partitions_def`; the registry's
  `cron_schedule` component can't express both together (same gap already
  recorded for demos/detroit-dwsd).
- `automation/legacy_orchestration_schedule.py` — a plain cron trigger over
  two unpartitioned assets; no `partitions_def` exists to build a
  partitioned-job schedule from (the legacy `ssis_workspace` component has
  no `partitions_def` field at all — see `component-feedback/2026-09-24-ssis-workspace-gaps.md`).
- `lakehouse_ingestion/template_vars.py`, `index_calculation/template_vars.py`,
  `customer_egress/template_vars.py`, `reporting/template_vars.py` — Jinja
  can't compose `AutomationCondition`s with `&`, so the composed values
  (and the shared `DAILY_PARTITIONS` instance) are exposed as template vars.

Not counted above (framework/component code, not `defs/`):
`components/demo_ssis_workspace.py`, `components/demo_iceberg_io_manager.py`,
`components/lakehouse_table_assets.py`, `components/graph_first_assets.py`,
`demo_data/legacy_ssis.py`, `demo_data/lakehouse.py`, `partitions.py`.

## Registry mapping (Axis 1 — every external system, a real component)

| System in the brief | Component | Notes |
|---|---|---|
| SQL Server / SQL Agent / SSIS (legacy) | `ssis_workspace` (community registry), subclassed as `DemoSsisWorkspaceComponent` | `action: execute` (Dagster orchestrates directly, replacing SQL Agent) + polling sensor on. 4 real gaps found and worked around — see `component-feedback/2026-09-24-ssis-workspace-gaps.md`. |
| Iceberg / Polaris (target lake) | `iceberg_io_manager` (community registry), subclassed as `DemoIcebergIOManagerComponent` | Genuinely Iceberg (PyIceberg SQL catalog), not a badge on DuckDB. 1 gap found (no namespace bootstrap) — see `component-feedback/2026-09-24-iceberg-io-manager-namespace-bootstrap.md`. |
| Price/corporate-action feed vendor | none — vendor unnamed in the brief | Conversation only; deterministic synthetic generators stand in. |

Both registry components required real, hands-on verification before use
(not just reading their schema) — the Iceberg component in particular was
tested end-to-end in a scratch project (local SQL catalog, partitioned
writes, read-back) before being wired into this build.

## Assumptions (flagged — not confirmed in the brief)

- 4h fail / 2h warn freshness window on `barrons_400_constituents`.
- 5:00 AM ET ingestion schedule (4.5h ahead of 9:30 AM ET market open).
- 90,000s (25h) freshness-lag threshold on the two legacy SSIS packages.
- A 60-symbol synthetic universe standing in for the one-index POC scope
  (not MarketGrader's full ~40,000-company universe).
- `kinds={"iceberg"}` verified against the installed `dagster-webserver`'s
  bundled icon set (`tool-iceberg-color.svg` present) rather than assumed.

## Validation

```bash
dg check defs && dg check yaml && dg list defs && dg list components
python validate_e2e.py
```

`validate_e2e.py` proves: the legacy SSIS chain materializes via a real
Dagster-triggered run (genuine orchestration against the demo-mode fake,
not just observation) and its native freshness checks pass; the observation
sensor still independently catches activity Dagster didn't trigger, with
the sensor/asset key-identity gap locked in by assertion; real rows land in
the Iceberg catalog for both raw assets across two partition dates; every
check evaluates and passes; the downstream chain materializes cleanly. See
`DEMO_SCRIPT.md` for the live run-of-show.
