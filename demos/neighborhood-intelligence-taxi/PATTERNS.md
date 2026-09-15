# Neighborhood Intelligence — Dagster Project Patterns

## What this document is

**This is your blueprint, not ours.** The reusable pattern for any
Dagster project at Neighborhood Intelligence — captures your team's
stack decisions and the Dagster shape that fits them. Every new
pipeline your team builds reads this first, then adds only what's
specific to itself (data sources, model names, schedules). The rules
here don't change from project to project; the brief does.

This file grew out of the NYC Yellow Taxi build (see
[WALKTHROUGH.md](./WALKTHROUGH.md) and
[AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md) for the concrete example)
and is meant to be extended by your team as you learn more.

**Make it always-on.** The natural next step is to package this file
as a Claude Code custom skill and publish it to your internal
registry — every engineer's Claude Code then auto-loads NI's
patterns in every Dagster project they touch, alongside
`/dagster-expert`. See [README.md](./README.md) § "How Claude always
finds PATTERNS.md" for the three tiers, from cloneable-per-project to
org-wide skill.

Reader flow:

1. **PATTERNS.md** (this file) — how any Dagster project at NI should be
   built. Read once, reference forever.
2. **AI_BUILD_PROMPT.md** — the per-project brief that references
   PATTERNS.md and adds project-specific bindings (which tables, which
   models, which cadence). Copy-pasted into Claude Code to generate a
   working project.
3. **WALKTHROUGH.md** — the same per-project brief, written as steps
   for a human engineer to follow by hand. Same output.

## Stack we're building on

- **Warehouse**: BigQuery
- **Transformation**: dbt with the `dbt-bigquery` adapter
- **Orchestration**: Dagster — `dagster-dbt` + `dagster-gcp` + the
  Dagster Community Components (DCC) registry
- **Reporting layer**: whatever the pipeline feeds (Looker, Power BI,
  Tableau, or a BigQuery table other systems read) — attach with the
  corresponding registry component
- **Deploy target**: Dagster+ Serverless

Fields under the same stack decision — auth via
`GOOGLE_APPLICATION_CREDENTIALS`, dbt models materialized into
per-layer schemas, monthly partitions as the default cadence unless a
brief calls for something else.

## Non-negotiable rules

1. **Registry components only, zero custom.** Every asset comes from a
   native `dagster-*` integration or a community component (via
   `defs.yaml`). Search first with `dagster-component search
   <keyword>`. Community registry:
   https://dagster-component-ui.vercel.app/.
2. **Escalation ladder — don't skip rungs.** Native `dagster-*`
   integration → community component as-is → community component
   subclassed (rung 3, only for component-internal seams a config field
   can't reach) → custom component from scratch (rung 4, **forbidden**
   for our builds). When a config field is missing, file a
   `component-feedback/` entry rather than writing a bespoke class.
3. **Predominantly YAML.** Custom Python belongs in `components/`, not
   `defs/`. Handwritten `@asset` functions are the anti-pattern.
4. **`post_processing:` before subclassing.** Every component accepts
   a top-level `post_processing:` block that overrides `AssetSpec`
   attributes on any target — `owners`, `kinds`, `tags`, `metadata`,
   `partitions_def`, `automation_condition`, `freshness_policy`,
   `group_name`, `description`, `code_version`. Target syntax uses
   Dagster asset selection (`group:silver`, `key:foo/bar`, `tag:x`).
   Reach for this before writing any subclass.
5. **dbt is the source of truth for the transformation layer** — see
   §"dbt conventions" below.
6. **Alerting is a Dagster+ platform capability, not code.** Native
   alert policies exist for Slack, Teams, email, and PagerDuty
   covering run failures, asset check failures, freshness violations,
   and schedule/sensor failures. Talk about them; don't build them.
7. **No planted failures.** Every asset materializes green. Checks
   exist so the talk-track can point at them; they earn their place
   against a green graph.

## dbt conventions

- **One `DbtProjectComponent` instance per dbt project.** Not one per
  medallion layer. A single instance covers silver / gold / report /
  whatever — group names come from dbt itself.
- **Groups from dbt's `+group:` config** in `dbt_project.yml`, declared
  in `models/_groups.yml`. `dagster-dbt` reads `config.group` from the
  compiled manifest and sets each asset's `group_name` automatically.
  No Dagster-side translator, no `meta.dagster.group_name` annotations.
- **Sources use `meta.dagster.asset_key`** to remap dbt source keys
  onto the bronze external asset keys. This is the one place dbt has
  to know about Dagster; every source entry gets one line.
- **Column tests + generic test macros surface as asset checks;
  singular tests do NOT.** If a reconciliation or cross-model
  constraint needs to show up in the Dagster UI, write it as a generic
  test macro (`macros/<name>_test.sql`) and reference it from a model's
  `_schema.yml` `data_tests:` block — never as a singular test in
  `tests/*.sql`. The generated asset-check name is
  `<test>_<model>_<args>_`.
- **Blocking severity via `config: { severity: error }`** in schema.yml
  on any test that should block downstream computation.
- **Dagster-only concepts** (`freshness_policy`, `automation_condition`)
  go in Dagster's `post_processing:` block, targeted by
  `target: "group:<name>"`. These have no dbt equivalent, so they
  belong on the Dagster side.

## Bronze / external-asset conventions

- **Use `external_bigquery_table`** for BigQuery source tables the
  pipeline reads but doesn't materialize. Declare each table as an
  observable external asset. This gives you a BigQuery-badged node at
  the top of the lineage graph without any ingestion code.
- **Set `owners`, `kinds`, and metadata inline** on the component (the
  first-class fields landed 2026-09-14). `post_processing:` is fine
  too and works identically.
- **Partition bronze assets to match downstream.** If the dbt models
  are monthly-partitioned, bronze should be too, so the partitions_def
  matches and Dagster can schedule end-to-end.

## Schedule conventions

- **Use the `cron_schedule` community component.** For partitioned
  pipelines set `partition_type: monthly` (or hourly/daily/weekly) +
  `partition_start`. The cron expression is optional in the
  partitioned path — cadence is inferred from the partitions_def and
  each tick auto-targets the most-recent finished partition.
- **`default_status: STOPPED`** on every schedule. RUNNING-on-deploy
  is user-hostile; the operator turns it on when they're ready.
- **`asset_keys:` lists only executable assets.** Bronze external
  assets are `AssetSpec`-only observations and can't be members of an
  asset job — the job takes silver as its root and bronze remains
  upstream in the lineage graph.

## Testing conventions

Three tiers of checks in every project:

1. **dbt column tests** in `_schema.yml` — `not_null`, `unique`, plus
   whatever else applies. Blocking on primary keys and any column
   downstream depends on structurally.
2. **dbt generic test macros** for cross-model constraints
   (row-count reconciliations, sum-equality, cross-source integrity).
   `equal_rowcount` (matching `dbt_utils.equal_rowcount`'s shape) is
   the standard row-count reconciliation macro.
3. **`enhanced_data_quality_checks`** DCC component for asset-level
   checks that don't fit dbt — anomaly detection, range checks,
   uniqueness across composite keys, custom SQL against a
   `bigquery_resource`. Only reach here when a dbt test doesn't cover
   it naturally.

Freshness policies go on the assets an on-call would page someone
about — typically the final report layer. Set via
`post_processing.attributes.freshness_policy` because freshness is a
Dagster concept without a dbt equivalent.

## Component reference — the DCC components most projects will use

| Component | What it does | Category |
|---|---|---|
| `external_bigquery_table` | Declare a BigQuery table as an observable external asset | external |
| `dagster_dbt.DbtProjectComponent` | Wrap a dbt project as a group of Dagster assets (native, not community) | dbt |
| `cron_schedule` | Schedule an asset selection on a cron (partitioned or not) | infrastructure |
| `enhanced_data_quality_checks` | Declarative asset checks — row_count, null, range, uniqueness, anomaly_detection, custom_sql | check |
| `bigquery_resource` | Register a BigQuery client for other components (e.g. `enhanced_data_quality_checks`) | resource |
| `freshness_check` | Attach a Dagster FreshnessPolicy to an asset for continuous SLA monitoring | check |

`dagster-component search <keyword>` finds everything else.

## Project layout

```
demos/<slug>/
├── AI_BUILD_PROMPT.md               # per-project brief (references this file)
├── WALKTHROUGH.md                   # human-executed steps
├── pyproject.toml                   # deps + hatch wheel force-include
├── src/<pkg>/
│   ├── __init__.py
│   ├── definitions.py               # load_from_defs_folder
│   ├── components/
│   │   ├── __init__.py              # re-export DCC classes for UI Components tab
│   │   ├── partitions.py            # shared partitions_def
│   │   ├── cron_schedule/           # installed via `dagster-component add`
│   │   └── external_bigquery_table/ # installed via `dagster-component add`
│   ├── defs/
│   │   ├── bronze/<source>/defs.yaml
│   │   ├── transformation/
│   │   │   ├── defs.yaml            # one DbtProjectComponent instance
│   │   │   └── template_vars.py     # partitions_def + freshness factories
│   │   ├── checks/<check>/defs.yaml # only if not covered by dbt tests
│   │   └── automation/defs.yaml     # cron_schedule
│   └── dbt_project/
│       ├── dbt_project.yml          # +group: per folder
│       ├── profiles.yml             # BigQuery target
│       ├── macros/                  # generic test macros
│       └── models/
│           ├── _groups.yml          # group declarations
│           ├── _sources.yml         # meta.dagster.asset_key remap
│           └── <layer>/*.sql + _schema.yml
└── dagster.yaml (optional)
```

## Deploy conventions

- **Slug uses dashes, package name uses underscores.** They differ.
  When calling `deploy_demo.sh` pass both:
  `./scripts/deploy_demo.sh <slug> <package_name>`. If you forget the
  second arg the deploy uploads fine and the code location fails to
  load with `ModuleNotFoundError` — the slug isn't a valid module
  name.
- **`[tool.hatch.build.targets.wheel]` in `pyproject.toml` must
  `force-include` `pyproject.toml` and `artifacts` the dbt `target/`
  and `defs/.local_defs_state/` paths.** Both are `.gitignore`d and
  hatchling drops gitignored files from the wheel by default —
  Dagster+ deploys load fine, then the code location fails at runtime
  with confusing missing-manifest errors.
- **`dg utils refresh-defs-state` before deploying** when using
  state-backed components (dbt, Fivetran) — or the location fails to
  load remotely.

## When you need to build a new project

1. Copy the [AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md) template as a
   starting point.
2. Fill in the project brief — data sources, model layout, group
   names, partition cadence, reporting destination.
3. Either run it through Claude Code (with the Dagster skills) or
   walk the [WALKTHROUGH.md](./WALKTHROUGH.md) steps by hand.
4. `dg check defs` → `dg dev` → ship.

## When you hit something PATTERNS.md doesn't cover

- **New external system:** `dagster-component search <keyword>` — the
  registry has ~975 components across 18 categories. Chances are
  high it exists. Add it via `dagster-component add <id>
  --auto-install`.
- **A component fits the domain but a config field is missing:**
  subclass (rung 3), file a `component-feedback/` entry describing
  the gap, and update this document with the subclass pattern once
  the fix lands upstream.
- **Genuinely novel stack combination:** the project's build spells
  out more in `AI_BUILD_PROMPT.md`, and once it ships, extract the
  reusable shape into PATTERNS.md so the next project inherits it.
  That's how this file grows.
