# Natural-Language Prompt — "Just Tell Claude What You Want"

The opposite bookend from [AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md).
That doc is a spec — precise, structured, exhaustive — the kind of
prompt an engineer writes to guarantee a reproducible build. **This
doc is what a person actually says.** No jargon, no YAML paths, no
component names, no ticket. Just describe the pipeline you want.

The reason this works — the reason the AI produces the same project
from three casual sentences as from a 120-line spec — is
[PATTERNS.md](./PATTERNS.md). PATTERNS carries the *how* (which
components, which conventions, which testing approach, which
deployment shape). The user only has to describe the *what*. That's
the demo moment.

## How to use

Point Claude Code at this project (so `CLAUDE.md` auto-loads
PATTERNS.md), then type one of the prompts below. Or install
PATTERNS.md as a global custom skill and Claude will apply it in any
project you're in.

## Minimal prompt — the "wow" version

Three sentences of prose:

> Hey Claude, I've got NYC taxi trip data in BigQuery at
> `bigquery-public-data.new_york_taxi_trips`. Build me a Dagster
> pipeline that pulls yellow trips and the taxi zone lookup, cleans
> them up, joins them into a trip-level fact table, and rolls up some
> daily and hourly aggregates plus a couple of report tables. Run it
> monthly and add some checks so I know the data's not broken.

That's it. Because PATTERNS.md is loaded, Claude knows to:

- Use `external_bigquery_table` from the DCC registry for the public
  dataset (not a hand-rolled ingestion asset)
- Wire the transformation layer as one `DbtProjectComponent` instance
  with `+group:` in dbt_project.yml driving Dagster group names
- Attach checks as dbt column tests + a generic `equal_rowcount`
  macro (not singular tests, which don't surface as asset checks;
  not hand-written `@asset_check` functions either)
- Use the community `cron_schedule` component with `partition_type:
  monthly`
- Put per-model tuning (freshness, automation) in `post_processing:`
  blocks
- Set `kinds: [dbt, bigquery]` on the dbt assets and `[bigquery]` on
  bronze
- Not build alerting (that's a Dagster+ platform capability)
- Not plant any failures
- Ship a working project on the first try

None of that came from the prompt. It came from PATTERNS.md.

## Slightly-shaped variant — add a specific constraint

If you have opinions beyond "pipeline that does X," add them inline
— still prose:

> Same as above, but partition monthly starting January 2022, and I
> want the final report tables to page us if they haven't refreshed
> in over a month. Blocking check on any row-count drop between the
> silver and gold layers — I've been burned by silent join drops.

The extra sentences add: partition start date, freshness policies on
the report layer, and a specific reconciliation check. Everything
else still comes from PATTERNS.md.

## Progressively-detailed variant — treat it as conversation

Nothing says the prompt has to be one shot. A more realistic user
flow is:

> **You:** Build me a Dagster pipeline over NYC yellow taxi data in
> BigQuery. Bronze → silver → gold → report, monthly.
>
> **Claude:** *[scaffolds project, reports back]*
>
> **You:** Nice. Add a reconciliation check on the trip fact — silver
> row count should equal gold row count, blocking if it doesn't.
>
> **Claude:** *[adds generic test macro + schema.yml reference]*
>
> **You:** And I want the report table to warn me if it's more than
> a month stale.
>
> **Claude:** *[adds freshness_policy via post_processing]*

Each turn is one sentence. PATTERNS.md handles everything you
*didn't* say.

## Why this works (the two-line pitch)

- **PATTERNS.md** captures your team's stack + conventions once.
- **Any AI prompt after that** is just "what's the specific pipeline
  I want?" — no need to re-specify the constraints every time.

Which means every future project at NI is a three-sentence ask, not
a hundred-line spec. Multiply that across a data team of ten
engineers building five pipelines a month, and PATTERNS.md pays for
itself in the first week.

## Related docs

- [PATTERNS.md](./PATTERNS.md) — the durable blueprint that makes
  natural-language prompts work
- [AI_BUILD_PROMPT.md](./AI_BUILD_PROMPT.md) — the spec-quality
  prompt for reproducible builds (same output, more explicit)
- [WALKTHROUGH.md](./WALKTHROUGH.md) — the dbt + DCC build path,
  done by hand
- [WALKTHROUGH_PYTHON.md](./WALKTHROUGH_PYTHON.md) — the pure Python
  build path, done by hand
- [README.md](./README.md) — orientation across all the docs
