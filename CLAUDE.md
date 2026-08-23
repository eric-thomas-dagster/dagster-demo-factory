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
| `briefs/` | One markdown brief per prospect. `_TEMPLATE.md` is the required shape. |
| `demos/<slug>/` | Generated Dagster projects, one directory per prospect. |
| `templates/demo_mode_pattern.py` | **Read before writing any demo component.** |
| `scripts/` | Build and deploy helpers. |
| `state/ledger.json` | What's been built. Prevents rebuilds. Always update it. |

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
