# dagster-demo-factory — house rules

This repo generates custom Dagster demo projects for sales prospects,
autonomously, overnight. Two Claude Code routines operate on it:
`Prospect Recon` (research → brief) and `Demo Factory` (brief → deployed demo).

## Demos vs POCs — two different artifacts

**This file governs DEMO builds.** A demo is synthetic, always-green, disposable,
and built for a 50-minute room from AE discovery notes.

**POC builds follow `docs/POC-PLAYBOOK.md`, which inverts several rules here** —
most importantly, POCs *must* be able to fail on demand, because induced failures
are the graded criterion. POCs are built from a customer's own scenario
specification, run against their real systems by their engineers, and live in
their own repo.

If you're unsure which you're building, you're building a demo. POC builds only
happen from an explicit POC scenario document, via the POC Builder routine.

## Who reads the output

A prospect's data engineering team, during an evaluation, on a shared screen,
with the ability to ask hard questions. They are deciding whether Dagster is
serious software. Sloppy generated code is worse than no demo.

## Skills

Use `dagster-expert` for every Dagster API decision and `dignified-python` for
every line of Python. Both live in `.claude/skills/` and are committed here
specifically because cloud routine sessions can only load skills from the
cloned repo.

## Two CLAUDE.md files — precedence

`dagster-component init` writes its own ~660-line `CLAUDE.md` into each
generated project. It's good reference — CLI usage, validation levels, common
gotchas, a task-to-component cheatsheet, and pointers to the registry
walkthroughs — and Claude Code loads it alongside this file when working in
`demos/<slug>/`. Keep it; it also helps the prospect if they clone the repo.

**But this file wins where they conflict**, and one conflict is guaranteed: the
generated doc has a section on asking the user clarifying questions before
generating, with an example dialog. That is right for interactive use and wrong
here. **Routine runs are unattended — never ask, never wait.** Where it says to
ask, take the answer from the brief; where the brief is silent, choose the
option most consistent with these house rules and note the choice in the
notification.

Its registry-usage guidance is authoritative and should be followed. Its
workflow guidance is not.

## Read LEARNINGS.md before building

`LEARNINGS.md` at the repo root holds verified facts from previous runs —
command forms, schemas, version quirks, known dead ends. Read it after this file
and before writing anything. Runs have no memory of each other; that file is the
only continuity.

It's a *maintained* file, not an append-only log. Every run invalidates wrong
entries, prunes what no longer earns its context cost, and only then appends
what it verified (step 10 of the Demo Factory prompt). Record only what you
confirmed by running it or reading source — a wrong entry is worse than none,
because the next run will trust it.

**Facts go in `LEARNINGS.md`; rules go here.** If something should change how
every future build behaves, it belongs in this file, not that one.

## Use `dg`, never `dagster`

Every command in these projects goes through the `dg` CLI. The legacy `dagster`
CLI is not to be used, even when it appears to work or when training data
suggests it.

| Use | Not |
|---|---|
| `dg dev` | `dagster dev` |
| `dg launch --assets '*'` | `dagster asset materialize --select '*'` |
| `dg launch --assets '*' --partition <key>` | `dagster asset materialize --partition` |
| `dg check defs` / `dg check yaml` | `dagster definitions validate` |
| `dg list defs` / `dg list components` | — |
| `dg utils refresh-defs-state` | — |

`dg launch` also takes `--partition-range <start>...<end>`, `--job`, and
`--config`. Note `dg launch` uses `--assets`; the legacy CLI used `--select`.
Getting this wrong wastes turns on flag errors.

## Demos run green — capabilities are shown, not triggered

**Default: everything materializes successfully.** Do not build a demo that
deliberately fails on screen. Eric's demo style is to show a working pipeline
and *talk through* the capabilities it has — checks, retries, alerting,
recovery — rather than stage a live failure and recover from it.

So build the capabilities in, fully configured and visible, and leave them
green:

- **Asset checks** — present, wired, passing. The prospect sees they exist,
  what they assert, and where the result surfaces. They don't need to see one
  go red to understand it.
- **Freshness policies** — configured on the assets they'd page someone about.
- **Retry policies** — *only where a source is genuinely flaky.* A
  decorative retry on a deterministic synthetic source invites a question we'd
  lose. Skipping one and saying why beats padding coverage.
- **Automation conditions** — showing what would recompute, and when.
- **Alerting** — *don't build it.* Dagster+ ships alert policies for Slack,
  Teams, email, and PagerDuty covering run failures, asset check failures,
  freshness violations, and schedule/sensor failures. Point at that in the UI
  and talk through it. Only write a custom sensor or hook when the brief names
  a use case Dagster+ alert policies genuinely don't cover — and say why in the
  notification when you do.

The talk track is *"here's what happens when this breaks"*, delivered against a
green graph. That's a stronger demo than a broken one and it removes the risk
of a staged failure not recovering live.

**Demos always work. There is no exception the brief can grant.**

Do not plant anomalies, corrupt partitions, seed missing data, or build any
scenario whose purpose is to make something fail. Not in demo mode, not behind
a flag, not "available if he wants it." A demo that can fail is a demo that
will fail, in front of the room, on the one path nobody rehearsed.

A brief asking for a planted failure is a brief that predates this rule —
**ignore that part of it and say so in the notification.** Only an explicit
instruction from Eric in the run's fire payload can override this, and even
then recovery is plain rematerialization, never a heal object.

The checks exist for when things break in *production*. That's the talk track.
Their value is entirely explicable against a green graph.

## Match the prospect's stack visually, even when execution is local

The engine can be DuckDB while the demo *looks* like their warehouse. Asset
`kinds` drive the icons and badges in the Dagster UI, and getting them right is
most of the visual fidelity for very little effort.

- On regular assets and `AssetSpec`s, set `kinds={"snowflake"}` (or
  `databricks`, `bigquery`, `redshift`) to match the prospect's stack. Max 3
  kinds per asset.
- **For dbt assets, kinds are derived from the manifest's `adapter_type`**, so a
  DuckDB-backed project badges everything `duckdb` by default. Override by
  subclassing the translator:

  ```python
  class DemoDbtTranslator(DagsterDbtTranslator):
      def get_asset_spec(self, manifest, unique_id, project):
          spec = super().get_asset_spec(manifest, unique_id, project)
          return spec.replace_attributes(kinds={"dbt", "snowflake"})
  ```

- Do the same for ingestion assets: badge them `fivetran`, `airbyte`, `s3`,
  `kafka` as their real stack dictates.
- This is presentation, not deception. It's demo mode — the point is showing we
  can orchestrate *their* stack, and `demo_mode: false` plus real credentials
  runs against the real thing. Keep the asset names, schemas, and structure
  honest; the badge just stops DuckDB from being a distraction.

## A demo must run with zero setup

After `git clone`, `uv sync`, and `dg dev`, the demo must work. No env vars to
set, no credentials, no manual file creation. That is the entire point of demo
mode.

- **Every demo-mode setting needs a working default.** Env vars may *override*
  configuration; they must never *gate* it. A required-with-no-default env var
  is a bug.
- Storage paths default to somewhere inside the project (e.g.
  `<project>/demo_data/demo.duckdb`), created on first use.
- If a demo needs a secret to run in demo mode, the demo mode is wrong.
- Verify by cloning fresh into a temp dir and running it. Working in the build
  directory proves nothing — that environment has state the prospect's doesn't.

## Dagster+ Serverless storage is ephemeral — plan for it

Each run executes in a fresh container. **Local files, including DuckDB
databases, do not persist between runs.** A code location reporting `LOADED`
only means the definitions parsed; it says nothing about whether assets
materialize.

The consequence matters for these demos: the fail → rematerialize → recover
sequence spans *multiple runs*, so with a local DuckDB path it will work
locally and silently break in Serverless.

So:

- **Give the live demo locally with `dg dev`.** That's where the interactive
  story works, and it's where you have control on a shared screen.
- **Dagster+ deployment proves the project is real** — it loads, the graph
  renders, the code location is genuine. Treat that as the proof point, not as
  the place you click through the recovery sequence.
- If a demo genuinely needs cross-run persistence in Dagster+, back the
  warehouse with something durable (S3-backed storage, MotherDuck, or the
  prospect's actual warehouse) and say so explicitly in the brief.
- **Say which mode each part of the demo runs in** in the README and the
  notification, so nobody discovers this on a shared screen.

## The feature floor is Dagster capabilities, not a fixed toolchain

Partitions, checks, freshness policies, automation conditions, metadata, and
kinds are Dagster capabilities — build those every time. **Which tools appear in
the demo comes entirely from the brief**, never from habit or from what's
convenient to build.

If the AE notes don't mention a tool, we don't assume it. No dbt unless they run
dbt. No Snowflake unless they run Snowflake. Where the brief marks a stack layer
`unknown`, prefer something generic over something specific-and-wrong.

### Orchestrating existing workloads — a first-class demo shape

Many prospects don't want new pipelines. They want Dagster to orchestrate and
observe what they already run — Fabric pipelines and notebooks, Databricks jobs,
Airflow DAGs, Synapse, stored procedures, cron. That is a *different demo* from
building a transformation graph, and it's often the more compelling one because
it's additive rather than a migration.

For that shape, reach for:

- **External assets / `AssetSpec`s** representing things Dagster didn't create,
  so their existing estate appears in the lineage graph.
- **Observable source assets** to report freshness on data Dagster doesn't
  materialize.
- **Pipes** for launching and streaming back from external compute.
- **Trigger-and-observe components** — the registry has real coverage here.
  Search it: `fabric_workspace`, `fabric_pipeline_trigger_job`,
  `fabric_lakehouse_resource`, `fabric_lakehouse_io_manager`, plus ~66 Azure
  and ~18 Databricks components. Don't build from scratch what already exists.

The story is *"your existing jobs, now with lineage, checks, freshness, and
declarative scheduling on top"* — not *"rewrite everything in Dagster."*

## Assets are idempotent — the source changes, not the asset

Recovery is never an action inside Dagster. There is no "heal" step, no reset
asset, no repair job. Rematerializing a partition re-reads the source and picks
up whatever is there now. That is how Dagster actually works, and the demo has
to behave the same way or it teaches the prospect something false.

**This means mocks simulate a source *system*, including arrival timing.** A
late-arriving feed is modelled as: the carrier's API has no rows for that
partition at 2pm, and has them at 4pm. The asset is unchanged and idempotent;
its input changed. Rematerialize and it succeeds — for the same reason it would
in production.

What follows from this:

- **No demo-control assets.** A disconnected `healed_partitions` or
  `demo_control` node with no lineage is the clearest possible tell that a
  prospect is looking at scaffolding, and the asset graph is the first thing
  they see.
- **No heal or reset jobs.** Op jobs are for genuine side-effectful work a
  prospect would recognise — shipping logs to an aggregator, firing a
  notification. They are not a place to hide demo state management.
- **Mock source state lives outside Dagster**, in `demo_data/`, representing the
  upstream system's own state. Dagster reads it; Dagster never writes it as part
  of the demo narrative.
- **Resetting the demo is an operation on the mock source**, done from a script
  or `make` target outside Dagster entirely — never a Dagster object.

If a node in the asset graph isn't something the prospect would recognise as
part of their own data flow, it doesn't belong there.

## Dagster feature floor — every demo, unless the brief forbids it

These are what make a demo look like Dagster rather than a DAG runner with nicer
colours. Include all of them unless a specific brief rules one out. If you have
to cut for time, cut *asset count*, not feature coverage — eight assets showing
all of this beats twenty showing none of it.

- **Partitions.** Time-based matching the prospect's real cadence. Reach for
  `MultiPartitionsDefinition` where a second dimension (region, carrier,
  tenant, source) is genuinely part of their domain. Partitions are the
  precondition for the targeted-recovery story, so this one is close to
  mandatory.
- **Asset checks.** At least three, each mapped to a pain named in the brief.
  At least one **blocking** severity, so downstream refuses to compute on bad
  input rather than computing something wrong. Warning-only checks reproduce
  the prospect's current situation with better styling and prove nothing.
- **A transformation layer built on whatever THEY use.** Not a default.
  Read the brief: if they run dbt, build a real dbt project (actual dbt Core,
  real `schema.yml` tests, real generated lineage — never mocked). If they run
  Fabric notebooks, Databricks jobs, Synapse pipelines, stored procedures, or
  bare Python, build *that*. A dbt demo for a shop that doesn't use dbt is the
  confident-wrong-guess failure, and it's obvious in the room.
- **Freshness policies.** On the assets a prospect would page someone about.
  This is the direct answer to "how would we know something broke."
- **Automation conditions.** `AutomationCondition.eager()` on the assets that
  should recompute themselves, plus at least one schedule where a real deadline
  exists. Declarative automation is a headline differentiator against Airflow's
  imperative scheduling — show it, don't describe it.
- **Asset metadata.** Row counts, dollar values, compliance tags — whatever the
  prospect's personas would actually look at.
- **Asset groups and kinds**, so the graph reads cleanly on a shared screen.

## YAML-first — the defs folder is mostly defs.yaml

**This is what we're selling.** A demo built out of hand-written Python asset
functions shows a prospect that Dagster is a Python framework. A demo built out
of `defs.yaml` pointing at components shows them a platform their whole team can
extend. The second is the product.

- **The `defs/` tree should be predominantly `defs.yaml`.** Aim for the large
  majority of definition files to be YAML. If most of `defs/` is `.py`, the
  build went wrong regardless of how good the Python is.
- **Custom Python belongs in a component**, not in `defs/`. Write the component
  class once under `components/`, then instantiate it as many times as needed
  from YAML. Per-asset Python functions in `defs/` are the anti-pattern.
- **Every `.py` file in `defs/` needs a one-line justification** in the
  notification saying why it couldn't be YAML. If you can't justify it, convert
  it.
- Report the YAML-to-Python file count in `defs/` in the notification, every
  run. A number you have to state is a number you'll notice.

The instinct to reach for Python is strong because it's faster to write. It is
also the thing that makes the demo argue against us.

## Migration prospects: show current AND future state

When a prospect is mid-migration — SSIS to Fabric, Airflow to something else,
on-prem to cloud — the strongest demo shows **both states in one asset graph**,
with lineage crossing between them.

Orchestrating the legacy estate they're moving *off* is not a distraction; it's
the whole reason the story is additive rather than a rip-and-replace. If the AE
notes mention what they're migrating from, build assets for it: trigger and
observe the legacy jobs, and show the new-platform assets downstream of them.

The line is *"Dagster orchestrates what you have today and what you're moving to,
during the migration, with one lineage graph across both."* That's a much harder
thing for a competitor to answer than a demo of the destination alone.

## One component instance, many objects — never one-per-object

**The scaling test: adding the prospect's 30th pipeline should be one more line
of YAML, not one more component instance or Python file.** If it isn't, the demo
argues against us — it says Dagster needs bespoke wiring per object, which is
exactly what they're trying to escape.

Reach for **workspace / collection components** that discover or enumerate many
external objects under a single instance, with an explicit mapping table in
`defs.yaml` binding each object to an asset spec. In the Databricks case that's
`assets_by_task_key`; Fabric, Airflow, and the other workspace-style components
have their equivalent. One instance, one YAML block, N assets.

**Reference implementation:**
`github.com/eric-thomas-dagster/databricks-workspace-bundles-demo` — public,
read it. Note especially how `defs/workspace_us/defs.yaml` maps many task keys
to asset specs with `key`, `deps`, `owners`, and `description` per entry, and how
adding a region is additive rather than multiplicative. Copy that shape.

Anti-patterns, all of which mean the build went wrong:

- N component instances for N external pipelines/jobs/notebooks
- A custom component whose job is "call this one specific pipeline"
- Per-object Python asset functions wrapping external triggers
- Any structure where adding an object means editing Python

If the explicit mapping is long, that's fine — a 40-entry `assets_by_task_key`
block is *good*, because it shows the prospect exactly how their estate maps in.

## Observe, don't just execute

A demo that only *triggers* external jobs is half a product. In the real world
someone runs that Fabric pipeline by hand, or a scheduled job fires outside
Dagster, and the prospect's question is always *"what happens when it wasn't
Dagster that started it?"*

**Every external-system component in a demo must observe as well as execute.**
Workspace-style components ship a polling sensor that detects externally
triggered runs and emits `AssetObservation` events — but **it is off by
default.** Turn it on:

```yaml
type: ...FabricWorkspaceComponent
attributes:
  generate_sensor: true     # alias for polling_sensor; default is false
```

Check for it on every workspace component you use. The convention is shared
across `FabricWorkspaceComponent`, `FivetranAccountComponent`,
`SnowflakeWorkspaceComponent`, `MLflowWorkspaceComponent`,
`DatabricksWorkspaceComponent`, and `PowerBIWorkspaceComponent` — though the
field name may vary, so read the component before assuming.

If a component you need has no observation surface, that's worth a
`component-feedback/` entry, and the sensor is worth adding in a subclass.

## The workspace-component convention

Workspace-style components across the registry follow one shape. Learn it once
and it applies everywhere:

- `@public` class
- a `translation:` callable field
- a `@public get_asset_spec(props)` hook — **the documented override point for
  customizing asset keys, tags, deps, and metadata**
- `polling_sensor` (alias `generate_sensor`), opt-in
- `defs_state` + `defs_state_config`
- `StateBackedComponent` inheritance, with enumeration in the state-write path
  so no HTTP fires at Dagster load time

That last point matters: **"it queries a live connection at load time" is not
true of these components and never a reason to reject one.** Enumeration happens
when state is written, not when definitions load. And `get_asset_spec(props)` is
the sanctioned way to give assets your own keys rather than the source system's.

## Rung 3 is not optional — subclass before you write custom

The most common failure is jumping from "the registry component doesn't fit
as-is" straight to a custom component, skipping the subclass rung entirely.

**These are NOT reasons to reject a registry component.** Every one is a
subclass away:

| Objection | Why it isn't disqualifying |
|---|---|
| "It discovers items from a live connection" | Demo mode exists to mock exactly that. Subclass, override the discovery/list call, return a fixed item list. Same seam as any other I/O boundary. |
| "Asset keys come from the source system, not us" | Add an explicit mapping in the subclass — `assets_by_task_key` in the Databricks workspace component is precisely this, and it's the established pattern. |
| "No partitions support" | Add the partitions config in the subclass and pass it through to the specs it builds. |
| "No freshness / retry / check surface" | Compose those from outside, or add the config fields in the subclass. |
| "It's a job, not an asset" | Ask whether a sibling asset-producing component exists first; if the trigger/poll body is what you want, wrap it rather than rewriting it. |

**Judge a registry component on its domain and its seam, not on its current
feature completeness.** If it covers the right system and has a method you can
override, rung 3 is the answer. A custom component is correct only when nothing
in the registry touches the domain at all, or when the shape is so wrong that a
subclass would override everything.

The test: *if the registry component gained two config fields, would it work?*
If yes, subclass it and note the two fields in the feedback file as the
suggested change. That feedback is worth far more than a bespoke class, because
it improves the component for everyone.

**Reference:** `github.com/eric-thomas-dagster/databricks-workspace-bundles-demo`
(public) subclasses the official Databricks workspace and asset-bundle
components to add a demo-mode seam, explicit `assets_by_task_key` mapping, and
op-naming fixes — while keeping the parent's behaviour in production. Its
`README.md` has a table of which subclasses are genuinely required and which
just add demo mode. That is the shape to copy.

## A justified custom component still follows the convention

When rung 4 is genuinely right — nothing in the registry touches the domain —
build it to the **workspace-component convention** above, not as a one-off. That
means: a `@public get_asset_spec(props)` override hook, an opt-in polling sensor,
`StateBackedComponent` inheritance with enumeration in the state-write path, and
a `translation:` field.

Two reasons. It must work with `demo_mode: false` against the customer's real
system, not just in the demo — otherwise the "point it at yours" moment fails.
And a component built to the convention can be contributed back to the registry,
which is how the gap actually closes.

Build it against the real API, then add the demo-mode seam on top. Building the
mock first and bolting real support on later produces something that only ever
works in the demo.

**The shape for any external job system is enumerate, execute, observe:**

1. **Enumerate** — list the system's jobs/packages/pipelines from its catalog or
   API into asset specs, in the state-write path so nothing fires at load time.
2. **Execute** — submit a run through the system's own API and poll it to a
   terminal state, surfacing its status, duration, and errors as first-class.
3. **Observe** — a polling sensor over the system's own run history, so runs
   started *outside* Dagster still produce `AssetObservation` events.

That triad generalizes to every job system — legacy schedulers, ETL tools,
notebook platforms, whatever the prospect runs. Find the API's equivalents of
those three operations and map them; the specifics differ, the shape doesn't.

## When you write a custom component, write feedback

If the escalation ladder falls all the way through to a custom component, the
community registry has a gap worth closing. Capture it while you know why.

Write `component-feedback/<YYYY-MM-DD>-<topic>.md` in this repo containing:

- **What was needed** — the capability, in one or two sentences.
- **What was searched** — the exact `dagster-component search` terms tried.
- **What came closest** — the component ID, and specifically **why it didn't
  fit**: no partition support, wrong auth model, missing a config field, schema
  too rigid, no demo_mode seam, whatever it actually was.
- **What was built instead** — a short description, and the file path on the
  branch.
- **Suggested change** — the smallest edit to the existing component that would
  have made it work, if there is one.

Be specific about the *why*. "Didn't fit" is useless; "no way to inject a
partition key into the request path" is actionable. This file is the input to
improving the registry, so vague feedback wastes the run's most valuable
byproduct.

**You may not assert a registry gap you did not search for.** The feedback file
must contain the literal commands you ran and what they returned. Reasoning like
"this is core Dagster, so there'd be nothing in the registry" is not permitted —
the registry has ~975 components including thin wrappers over core Dagster
calls (`cron_schedule`, `interval_schedule`, `automation_condition_applicator`
all exist). Search first, every time, with `--json`. If a search you claimed to
run isn't in the feedback file, the custom component is unjustified.

## Component escalation ladder

Do not skip a rung:

1. Native Dagster integration, in `defs.yaml` component form
2. Community registry component, as-is
3. Community registry component, subclassed
4. Custom component written from scratch — **last resort**

Rung 4 is the most expensive and the likeliest to eat the build window. Run at
least three distinct registry searches before writing one, and record the gap
in `LEARNINGS.md`.

## Layout

| Path | Purpose |
|---|---|
| `docs/RUNBOOK.md` | **How to operate this — read this first if you're a human.** |
| `docs/POC-PLAYBOOK.md` | Rules for POC builds. Inverts several rules in this file. |
| `pocs/<slug>/` | POC projects, when a standalone repo couldn't be created. |
| `component-feedback/` | One file per registry gap, written whenever a build falls through to a custom component. |
| `briefs/` | One markdown brief per prospect. `_TEMPLATE.md` is the required shape. |
| `demos/<slug>/` | Generated Dagster projects, one directory per prospect. |
| `templates/demo_mode_pattern.py` | **Read before writing any demo component.** |
| `scripts/` | Build, validate, preflight, deploy, and reset helpers. |
| `state/ledger.json` | What's been built. Prevents rebuilds. Always update it. |
| `docs/00-SETUP.md`, `docs/01-*`, `docs/02-*` | Reference copies of setup notes and the routine prompts. **Nothing reads these** — the live prompts are in the routine web UI. |

## Non-negotiables

**Native integrations first.** `dagster-dbt`, `dagster-snowflake`,
`dagster-fivetran`, `dagster-sling`, etc. Reach for the community registry only
where no native integration covers the prospect's tool.

**Components and YAML, not raw Python defs.** Where a component exists, use
`defs.yaml`. The components workflow is a large part of what we're selling.

**Search the registry, don't recall it.** ~975 components across 18 categories.
`dagster-component search <term>` then `info <id>`. A guessed component ID
wastes the run.

**demo_mode subclasses, never reimplements.** See
`templates/demo_mode_pattern.py`. Asset keys, specs, partitions, metadata,
checks, and YAML schema are identical in both modes. Only the network boundary
is faked. This is the rule everything else depends on.

**Deterministic synthetic data.** Seed every generator. Row counts must not
drift between runs.

**Plant one failure.** Every demo needs an anomaly that a real asset check
catches. An all-green asset graph doesn't demonstrate data quality tooling.

**Validate before publishing.** `dg check defs`, `dg list defs`, `dg check
yaml`, and a real end-to-end materialization must all pass.

**Every build ships `validate_e2e.py` at the project root.** `dg launch --assets
'*'` exits immediately on any partitioned asset, and partitions are near
mandatory here, so a generic runner can't gate these projects — the build knows
its own partition keys and must provide the harness. It builds the implicit job
via `defs.resolve_implicit_job_def_def_for_assets(asset_keys)`, executes each
partition with `job.execute_in_process(instance=..., partition_key=k,
asset_selection=...)`, and exits non-zero on any failure.
`scripts/validate_demo.sh` calls it.
A project that loads but crashes on materialize must not be deployed.

**Their vocabulary, not ours.** Assets named `member_eligibility_daily`, not
`staging_table_2`. Pull the nouns from the AE's discovery notes.

**Scope down rather than ship broken.** A clean 8-asset demo beats a broken
22-asset one. If time is running out, cut a branch of the graph and say so.

## Secrets

`DAGSTER_CLOUD_API_TOKEN` and `GH_TOKEN` come from the cloud environment. Never
print them, never commit them, never put them in a PR body or email. Never
commit a `.env` file. Demo projects reference credentials only as
`{{ env.VAR_NAME }}` in YAML — and since demos run in demo mode, those env vars
are expected to be absent.

## Git

Branches are `claude/`-prefixed. Never push to `main`; open a PR. The PR body
for a demo is the run-of-show: what to click, in what order, what to say.

## Deployment

`dagster-cloud serverless deploy-python-executable ... --build-method local`.
Never `serverless deploy` — that path needs a Docker daemon, which the routine
sandbox doesn't have.

Location names are `demo-<slug>` so they can be found and reaped later.
Deployment target comes from `$DAGSTER_CLOUD_DEPLOYMENT`.

A successful deploy command does not mean the code location loaded. Poll and
confirm before reporting success.
