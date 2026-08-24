# dagster-demo-factory — house rules

This repo generates custom Dagster demo projects for sales prospects,
autonomously, overnight. Two Claude Code routines operate on it:
`Prospect Recon` (research → brief) and `Demo Factory` (brief → deployed demo).

## Who reads the output

A prospect's data engineering team, during an evaluation, on a shared screen,
with the ability to ask hard questions. They are deciding whether Dagster is
serious software. Sloppy generated code is worse than no demo.

## Skills

Use `dagster-expert` for every Dagster API decision and `dignified-python` for
every line of Python. Both live in `.claude/skills/` and are committed here
specifically because cloud routine sessions can only load skills from the
cloned repo.

## Read LEARNINGS.md before building

`LEARNINGS.md` at the repo root accumulates verified facts from previous runs —
correct command forms, component schemas, registry gaps, known dead ends. Read
it after this file and before writing anything. Runs have no memory of each
other; that file is the only continuity.

Every run updates it (step 10 of the Demo Factory prompt), including failed
runs. Record only what you verified by running it or reading source — a wrong
entry is worse than none, because the next run will trust it.

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
- **Retry policies** — set where a flaky source would justify them.
- **Automation conditions** — showing what would recompute, and when.
- **Alerting hooks / sensors** — configured, pointing somewhere plausible.

The talk track is *"here's what happens when this breaks"*, delivered against a
green graph. That's a stronger demo than a broken one and it removes the risk
of a staged failure not recovering live.

**Only stage a live failure when the brief explicitly asks for it**, under
`Failure demonstration: yes`. It's opt-in, never the default. When it is asked
for, recovery is still plain rematerialization — see the idempotency rule
below.

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
- **A real dbt project.** Actual dbt Core executing against DuckDB via
  `dagster-dbt` — real models, real `schema.yml` tests, real generated
  lineage. Do not mock dbt. Most prospects run dbt, its lineage is the most
  legible thing in the UI, and simulated lineage collapses under a follow-up
  question.
- **Freshness policies.** On the assets a prospect would page someone about.
  This is the direct answer to "how would we know something broke."
- **Automation conditions.** `AutomationCondition.eager()` on the assets that
  should recompute themselves, plus at least one schedule where a real deadline
  exists. Declarative automation is a headline differentiator against Airflow's
  imperative scheduling — show it, don't describe it.
- **Asset metadata.** Row counts, dollar values, compliance tags — whatever the
  prospect's personas would actually look at.
- **Asset groups and kinds**, so the graph reads cleanly on a shared screen.

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
yaml`, and an actual `dg launch --assets '*'` must all pass.
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
