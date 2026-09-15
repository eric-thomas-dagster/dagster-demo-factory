# Neighborhood Intelligence — NYC Yellow Taxi (Dagster + BigQuery + dbt)

A working Dagster project orchestrating a bronze → silver → gold →
report pipeline over the public `bigquery-public-data.new_york_taxi_trips`
dataset. Built with the Dagster Community Components registry and
`dagster-dbt`. **Zero custom components.**

## Files in this project

| File | What it is | Read it when |
|---|---|---|
| [PATTERNS.md](./PATTERNS.md) | Reusable blueprint — how *any* Dagster project at Neighborhood Intelligence should be built. Rules and conventions that stay the same from pipeline to pipeline. | **Start here.** Read once, reference forever. Extend as your team learns. |
| [AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md) | The per-project brief for this pipeline, phrased as a prompt for Claude Code. References PATTERNS.md and adds only what's specific to NYC Yellow Taxi. | You want to build a new pipeline by giving Claude Code a brief and letting it generate the project. Use this as a template — copy, edit the brief, run. |
| [WALKTHROUGH.md](./WALKTHROUGH.md) | The same build described as steps a human engineer types by hand, with full code snippets. | You want to walk the build yourself, learn the pattern, or narrate it to someone else. |
| [src/](./src/) | The generated project — the artifact both paths above produce. | You want to run `dg dev`, click through the graph, and see the thing work. |

## Suggested flow

1. **Read [PATTERNS.md](./PATTERNS.md) once.** It's your team's
   blueprint — captures how a Dagster project should be structured at
   NI, given your stack (BigQuery + dbt + Dagster+ Serverless).
2. **Pick a path per new pipeline:**
   - Faster + hands-off → copy [AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md),
     rewrite the brief for your new pipeline, run it through Claude Code.
   - Slower + more control → follow [WALKTHROUGH.md](./WALKTHROUGH.md)
     step by step, adapting each step to your new pipeline.
3. **Extend PATTERNS.md** whenever your team hits something the
   blueprint doesn't cover. That's how it stays current.

## How Claude always finds PATTERNS.md

Three tiers, from lightest to most durable — pick the one that
matches how you want your team working:

### 1. This project only — `CLAUDE.md` at project root

Claude Code auto-loads the `CLAUDE.md` file at the project root on
every session. This project's `CLAUDE.md` opens with a prominent
"read PATTERNS.md first" block — so any Claude session working in
this directory (interactive edits, AI-driven pipeline builds, code
review, whatever) starts with PATTERNS.md as the authoritative
reference. Works out of the box; nothing to install.

### 2. Any project cloned from this one — carry it forward

If your team clones this repo as the seed for a new NI pipeline
(rename, edit the brief in `AI_BUILD_PROMPT.md`, off you go), both
CLAUDE.md and PATTERNS.md come along — the guarantee holds without
extra setup. Same for the AI-driven build path: the prompt opens
with `Read PATTERNS.md at the project root...` so a fresh
Claude Code session ingests the blueprint before scaffolding.

### 3. Every Dagster project your team touches — package PATTERNS.md as a custom Claude skill

The productionized version. Wrap PATTERNS.md as a Claude Code custom
skill and publish it to your internal package registry (or a private
GitHub repo). Any engineer at Neighborhood Intelligence installs it
once:

```bash
claude plugin install github.com/neighborhood-intelligence/dagster-patterns-skill
```

From then on, **every Claude Code session — in every project, every
directory** — auto-loads your PATTERNS.md alongside `/dagster-expert`.
An engineer scaffolding a brand-new Dagster project in an empty
directory gets NI's conventions applied without having to remember
to reference PATTERNS.md at all.

This is the "make it stick" tier: PATTERNS.md becomes as ambient in
your team's tooling as `/dagster-expert` already is. It's how you
turn today's demo into your org's standard for every future pipeline.

The skill packaging is a small repo with a manifest that points at
PATTERNS.md as its always-loaded context; the Dagster team can help
you scaffold it when you're ready.

## Where PATTERNS.md gets its expertise from

PATTERNS.md captures two kinds of things:

- **Business-specific decisions:** your warehouse choice (BigQuery),
  your transformation tool (dbt), your reporting layer, your team
  ownership structure, your partition cadence defaults. These belong
  to Neighborhood Intelligence — nobody else can decide them for you.
- **Dagster best practices:** registry-components-only, `post_processing:`
  before subclassing, one `DbtProjectComponent` instance per project,
  generic-test-macros-not-singular-tests, etc. These aren't NI-specific;
  they're how any well-shaped Dagster project should look.

For that second bucket — the general Dagster expertise — the
**`/dagster-expert`** Claude Code skill is the authority. It knows
asset patterns, automation conditions, schedules and sensors, project
layout, and every official `dagster-<vendor>` integration in depth.
The `dagster-integrations` sub-skill covers the specific components.

Two ways to use them together:

- When writing or extending PATTERNS.md, invoke `/dagster-expert` to
  double-check that a rule you're writing down actually reflects
  current Dagster best practice.
- When building a project (either path), the AI has both PATTERNS.md
  (your business shape) and `/dagster-expert` (Dagster's general
  expertise) available — they compose. PATTERNS says what to build;
  `/dagster-expert` fills in the how at the level of individual
  component config.

## Running this project

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export NIT_BIGQUERY_PROJECT=your-gcp-project
export NIT_BIGQUERY_DATASET=nit_taxi
uv sync
uv run dg dev
```

Then open http://localhost:3000 and materialize the graph.
