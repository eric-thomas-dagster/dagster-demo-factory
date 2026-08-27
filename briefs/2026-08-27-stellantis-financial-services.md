---
company: "Stellantis Financial Services"
slug: "stellantis-financial-services"
domain: "stellantis-fs.com"
demo_date: "TBD"        # AE doc: "Demo is being scheduled. Hopefully later the week of Aug 24th" — still no meeting on calendar as of this rewrite (checked 2026-08-27)
demo_time: ""
attendees:
  - "Nikitas (Nick) Gogos — Enterprise Data Management lead (new to company, ~3 months) — email unknown, found via LinkedIn Sales Navigator"
  - "Chris Rodriguez — leads enterprise data platform effort / software development leader — email unknown, found via LinkedIn Sales Navigator"
ae_doc: "https://docs.google.com/document/d/1NDlJt9BdhrnwfN6u3j3SRQ3F0zOvjAWJIPqi-YUKcuE/edit"
ae_doc_modified: "2026-08-24T14:57:11.995Z"
overall_confidence: "medium"
generated: "2026-08-27T00:10:00Z"
---

# Stellantis Financial Services — demo brief

<!-- REWRITE 2026-08-27 (fifth pass): Honouring
requests/stellantis-financial-services.md (action: rebuild-brief, requested
2026-08-26): "Focus on SSIS coexistence — the last build ignored the legacy
side entirely. Demo shape is migration-both-states."

Audited the deployed build directly before rewriting anything (PR #11,
"The 700th Package", currently live at demo-stellantis-financial-services).
The prior brief's fourth pass *labeled* two bronze assets
(`raw_dealer_floorplan_feed`, `raw_credit_bureau_pull`)
`legacy_system_boundary: homegrown_scheduler_owned` in `defs.yaml` metadata —
but both are entries in the same `assets_by_item_name` mapping table as every
other asset, executed through the identical subclassed `fabric_workspace`
component, triggered the identical way (Dagster calls the mocked Fabric
trigger/poll API). The only difference from a fully-migrated asset is that
the polling sensor's fixed mock list (`external_run_history.py`) happens to
include one run for each of them, framed as "SFS's own scheduler." That is a
sensor blip and a metadata string, not a second execution path Dagster
doesn't own. Confirmed by reading `fabric_pipelines/defs.yaml` and
`external_run_history.py` directly (both files shown above this comment
block's research, re-read 2026-08-27) — there is no code path in the entire
project where a legacy system, not Fabric, produces an asset's data. "The
last build ignored the legacy side entirely" is accurate, not a
harsh read.

Research (AE doc, job postings, public signals, conflicts) is unchanged from
the fourth pass and re-verified today: same `ae_doc_modified` timestamp, no
new calendar event, no new Drive material (checked via calendar search and
Drive search, 2026-08-27). This rewrite's only substantive change is the
Build directives, specifically Demo shape, Asset graph, Component strategy,
and Money shot — bringing the demo shape explicitly to **migration-both-states**
per the request, instead of `orchestrate-existing-workloads`'s coexistence
pattern applied only as metadata. Two of the five bronze feeds become
*genuinely* legacy: no Fabric pipeline behind them at all, materialized only
by SFS's own homegrown scheduler (never by Dagster), observed into the
lineage graph by a dedicated sensor over that system's own run log — and
consumed downstream by Fabric-side assets, so the graph shows one continuous
lineage line crossing from the system they're leaving to the system they're
moving to. That crossing is the whole ask. -->

## Demo thesis

Stellantis Financial Services' Enterprise Data Management lead (Nick, 3 months in role) is
racing to migrate ~700 homegrown SSIS packages onto Microsoft Fabric while the team triples in
size, and their single biggest complaint about the system they built themselves is that failure
recovery is manual and replay is weak — with SFS now underwriting up to eight auto ABS
securitization deals in 2026, an audit-ready, loan-level data tape is not optional. They are
evaluating Dagster as the orchestration layer *on top of, or replacing*, their homegrown system —
not asking for a new transformation stack: their bronze/silver/gold logic already exists, inside
the SSIS packages and stored procedures they're migrating into Fabric pipelines. This demo has to
prove Dagster can sit across *both* halves of a migration that is nowhere near finished: the SSIS
packages still running exactly as they do today, under SFS's own scheduler, and the pipelines
already cut over to Fabric — in **one lineage graph**, with data flowing from the legacy side into
the migrated side, not two disconnected demos glued together with a label. It also has to prove
Dagster can wrap their *actual* Fabric pipelines with lineage, blocking checks, freshness, and
partition-scoped execution — every partition green, every check passing — with "replay is weak"
and "recovery is manual" answered by what the platform *shows*, not by a staged break-and-fix. If
Nick and Chris leave believing "Dagster orchestrates what we're leaving *and* what we're building,
today, in the same graph — we don't have to finish the migration before we get value, and we don't
have to start everything through Dagster on day one," the demo has done its job.

## Meeting

- **When:** Not booked yet — AE (Austin) doc says "hopefully later the week of Aug 24th." No
  calendar event found on a fresh search as of this rewrite (checked a fifth time, 2026-08-27 —
  same result as all four prior passes). Intro call already happened Aug 21, 2026 (Gong-recorded).
- **Who's in the room:** Nick Gogos (Enterprise Data Management lead, new to SFS, ex-Databricks
  background, familiar with Airflow conceptually — cares about scalability, SLA/lateness
  visibility, and replacing homegrown tooling before it becomes an operational risk). Chris
  Rodriguez (leads the enterprise data platform effort, software development leader — cares about
  architecture fit for the Fabric migration, dependency handling, and dynamic metadata passing
  between pipeline steps).
- **Meeting type:** First tailored technical demo, following an initial intro/discovery call.

## Use case — confidence: high

SFS is building a new enterprise data platform on Microsoft Fabric while migrating a large
legacy SQL Server / SSIS environment (~700 SSIS packages) that currently runs on a
configuration-driven orchestration layer they built in-house. They are evaluating whether Dagster
can become the production orchestration layer on top of (or replacing) that homegrown system —
**not** whether Dagster can also become their transformation engine. Stated ask for the SE: model
their vendor-file → bronze → silver → gold flow and show the visibility, dependency handling, and
partition-scoped operations their homegrown layer lacks. Because the migration itself is ongoing
and unfinished (~35-person team, up from ~7), any demo that shows only the Fabric side implicitly
assumes the migration is done — which is the opposite of where they actually are.

## Current stack — confidence: high (source: AE discovery notes, corroborated by public job postings)

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Homegrown config-driven layer (evaluating Dagster vs. Airflow/Astronomer conceptually) | AE notes |
| Warehouse / platform | Microsoft Fabric (target), legacy Azure Synapse (prior gen) | AE notes |
| Transformation | SQL Server stored procedures / SSIS (~700 packages), bronze/silver/gold medallion pattern — **no dbt mentioned anywhere, in AE notes or in public research re-checked 2026-08-27** | AE notes |
| Ingestion | Vendor file drops (dealer, servicer, credit-bureau feeds implied) via SSIS | AE notes |
| BI | Not stated by AE; Power BI confirmed elsewhere in Stellantis job postings (and is Fabric-native) | inferred, medium |
| Cloud | Microsoft Azure (Fabric is Azure-native) | AE notes / job postings |
| CI/CD | Unknown | — |

Job postings for the separate "Customer Data Platform" team list Azure, Snowflake, BigQuery, and
AWS as acceptable cloud experience, and Python/Spark for pipelines — broader than the
Fabric-only picture Nick and Chris described. See Conflicts and gaps.

## Pain — confidence: high

Direct AE quotes: *"Homegrown orchestration works, but failure recovery is manual, replay is weak,
UI/visibility is fragmented, and the solution is not as scalable or flexible as they want."*
Desired future state, in their words: *"single pane of glass across the full data flow, real-time
alerting, historical reporting/KPIs, SLA tracking, replay/backfill, and clear failure/root-cause
visibility."* They also flagged interest in *"asset-level expected timing / lateness visibility and
downstream impact awareness"* and passing *"dynamic metadata/payloads between steps (e.g., vendor
file date)."* Note: their own notes mention "potentially self-healing workflows" as a longer-term
interest — that stays a talk-track line about the platform's model, not something this build stages
live; see Demo mode below.

## Data domains

Auto loan and lease originations, payment/collections transactions, delinquency events, dealer
floorplan financing, and — per SFS's 2026 capital strategy (public) — data feeding up to eight
prime/near-prime auto ABS and lease ABS securitization deals this year. (Stellantis Financial
Services Italia also closed a €1.2B public auto-loan securitization on 2026-06-30 — a second,
independent public data point that the ABS-pool-accuracy stakes are real and not brief-invented.)
Scale/cadence not stated by AE; ~700 SSIS packages and a team grown from ~7 to ~35 developers
implies daily-batch, many-source ingestion at meaningful volume. Treat volumes as unknown/low-
confidence and use industry-typical auto-finance cardinalities (thousands of contracts/day, dozens
of dealer/vendor feeds).

## Public signals

- **Job postings:** At least 2 distinct open data roles found — a Data Engineer on the "Customer
  Data Platform" team (Auburn Hills; Azure/Snowflake/BigQuery/AWS, Python/Spark) and a Senior
  Finance Data Engineer/Data Analyst supporting FP&A (Power BI-based reporting). Two open reqs
  across two different data teams is a real, if modest, signal of active investment. Re-checked
  2026-08-27: no posting for either team mentions dbt, Airflow, or any named transformation
  framework beyond "Python/Spark pipelines" — reinforces treating bronze/silver/gold as
  stored-proc/SSIS logic, not a dbt project waiting to be written.
- **Engineering blog / GitHub:** None found for SFS specifically. Parent Stellantis has a public
  GitHub org, but it's connected-vehicle API docs — unrelated to the financial services back
  office.
- **Recent news:** SFS is the automaker's captive finance company, built from the 2021-22
  acquisition of First Investors Financial Services Group (explains the legacy SQL Server/SSIS
  stack — that's inherited infrastructure, not something SFS built from scratch). SFS is
  highlighted as a "strategic growth engine" in Stellantis's May 2026 FaSTLAne 2030 plan, and per
  Auto Finance News (2026 capital strategy), SFS plans up to eight auto/lease ABS securitizations
  in 2026. Parent Stellantis N.V. took analyst downgrades in late August 2026 (UBS, Bernstein) on
  U.S. turnaround/tariff concerns and is weighing the Brampton plant's future — none of that is
  about the finance arm's data platform decision and shouldn't be read into the room, but it's
  worth knowing the parent is under cost pressure if budget scrutiny comes up.
- **Industry / compliance:** GLBA / FTC Safeguards Rule applies directly — SFS is a "financial
  institution" under GLBA as an auto lender, with a written information-security-plan obligation
  around nonpublic personal financial information. ABS securitizations bring investor and
  rating-agency scrutiny of loan-tape accuracy (SEC Reg AB shelf-registration context) — a second,
  independent reason for auditable lineage beyond GLBA.
- **Orchestration signals:** Clear homegrown-orchestrator replacement pattern (the exact profile
  Dagster's asset-centric pitch is built for), an active platform migration (Fabric) still in
  progress rather than complete, and a team that tripled in size — all consistent with a genuine
  re-platforming budget rather than a kick-the-tires eval.

## Conflicts and gaps

- **Cloud stack breadth vs. AE notes:** AE notes describe a Fabric-centric target platform; public
  job postings for a different SFS data team (Customer Data Platform) list Azure, Snowflake,
  BigQuery, and AWS as acceptable experience. This could mean multiple parallel data efforts
  inside SFS with different stacks, or the JD is generic boilerplate not yet updated for the
  Fabric decision. Flagging both rather than picking one — worth a clarifying question live.
- **No confirmed emails or exact titles beyond AE's paraphrase** — Nick's and Chris's exact job
  titles are the AE's characterization, not verified against LinkedIn directly (Sales Navigator
  links were provided but not fetched as part of this research pass).
- **No stated hard timeline or budget** — AE notes explicitly say "no hard purchase date stated."
  Do not imply urgency in the room beyond what they've said themselves.
- **Volumes and SLAs are unknown** — the brief uses industry-typical auto-finance assumptions;
  flag this as a gap rather than presenting invented numbers as fact.
- **No meeting booked yet** — re-checked calendar and Drive again for this rewrite (2026-08-27),
  still nothing scheduled and no new AE material beyond the same Aug 24 doc (unchanged since all
  four prior passes). The AE notes mention a "wish list" the prospect offered to share; it still
  hasn't shown up in Drive.
- **Which specific packages are still legacy is a brief-level assumption, not an AE-confirmed
  fact.** The AE notes say ~700 SSIS packages exist and a Fabric migration is "actively underway,"
  but don't say which packages have cut over yet. This brief assumes dealer-floorplan financing and
  the credit-bureau pull — two third-party-integration-heavy, harder-to-migrate feeds — are still
  legacy, while the core loan/lease/payment flow has already moved. That's a plausible migration
  sequencing (core lending data first, brittle external integrations last), not a stated fact — say
  so if asked which packages are actually still on SSIS.
- **Failure/recovery framing in the AE's own ask** — the AE notes literally say the SE should
  "show how operators recover from a failed step without rerunning everything," and separately
  mention the prospect's own longer-term interest in "self-healing workflows." Per house rules
  (`CLAUDE.md`, "Demos always work — there is no exception the brief can grant"), this build
  does **not** stage a live failure to satisfy that line item. It answers the same pain — replay,
  targeted reprocessing, root-cause visibility — by demonstrating partition-scoped
  rematerialization live against clean data, and by walking through what the blocking checks and
  freshness policies would do in production, against a green graph. Flagging this explicitly
  because it's a real gap between what the AE wrote down and what gets shown: say so if asked why
  nothing broke on screen — "we build it to show you the mechanism, not to fake a fire."

---

# Build directives

## Asset graph

Model the vendor-file → bronze → silver → gold flow the AE notes ask for, themed on auto-loan/
lease servicing feeding ABS pool reporting — **as two systems in one graph**: the SSIS packages
still running exactly as they do today under SFS's own homegrown scheduler, and the pipelines
already cut over to Fabric. Partition on a `date` (daily) × `dealer_group` (a handful of regional
dealer groups, e.g. `midwest`, `northeast`, `south`, `west`) `MultiPartitionsDefinition` for the one
asset where a second dimension is genuinely part of their domain — dealer floorplan feeds arrive
per-region — so partition-scoped, targeted-reprocessing operations are demonstrable live without
needing anything to be broken first.

**Legacy (SSIS / SFS's own homegrown scheduler — not yet migrated). Dagster never triggers these;
it only observes them, because SFS's own scheduler stays master for as long as they're on SSIS:**
- `raw_dealer_floorplan_feed` — `date` × `dealer_group` (4 regions) — the one asset with a genuine
  second partition dimension. Third-party dealer-integration feed; a plausible last-to-migrate
  candidate (see Conflicts and gaps).
- `raw_credit_bureau_pull` — daily. External bureau integration; same reasoning.

**Bronze (already-migrated, Fabric-pipeline-triggered vendor ingestion):**
- `raw_loan_originations` — daily
- `raw_lease_originations` — daily
- `raw_payment_transactions` — daily

**Silver (Fabric-pipeline-triggered conforming/staging — represents the existing SSIS/stored-proc
logic, now running as Fabric pipelines). Two of these depend directly on the still-legacy bronze
assets above — this is where the graph's lineage crosses from the system they're leaving into the
system they're building:**
- `stg_loan_originations`
- `stg_lease_originations`
- `stg_payment_transactions`
- `stg_delinquency_events`
- `dim_dealer` — depends on `raw_dealer_floorplan_feed` (**legacy**), rolling up all 4
  `dealer_group` partitions for a date via a multi-to-single-dimension partition mapping. This
  asset is entirely Fabric-migrated, but its only input is still produced by SSIS today.
- `dim_borrower` — depends on `stg_loan_originations`, `stg_lease_originations` (migrated), and
  `raw_credit_bureau_pull` (**legacy**) — a second, independent boundary crossing.

**Gold (Fabric-pipeline-triggered marts):**
- `fact_loan_portfolio`
- `fact_delinquency_snapshot` — freshness policy (this is the one someone gets paged over)
- `abs_pool_eligibility` — freshness policy + blocking check; the money-shot terminal asset,
  directly tied to SFS's 2026 ABS securitization calendar
- `gl_reconciliation_summary`
- `customer_360` — echoes the "Customer Data Platform" job posting's own framing

**Reporting:**
- `powerbi_portfolio_dashboard_refresh` — Fabric-native BI layer, represents the exec-facing
  dashboard

17 assets total (2 legacy, 15 Fabric-migrated), within the 12–25 target, spanning an unfinished
migration → Fabric pipeline lineage → BI — all as systems they already run or are actively cutting
over, not a rewrite. Do not model all ~700 legacy packages individually; two representative,
concretely-legacy assets crossing into the migrated estate make the point without turning this
into a 30-asset graph — see Explicitly out of scope.

## Fidelity

**Data-backed**, not graph-first. This demo needs real values on screen: the reconciliation and
completeness checks must compute their pass/fail from actual synthesized rows (not always pass by
construction the way a `pass`-bodied asset would), the dealer-floorplan lateness check needs a real
arrival timestamp to compare against, and the money-shot's ABS-pool and delinquency numbers need to
look like a real loan portfolio when Nick and Chris ask "how many contracts is that." Back it with
DuckDB standing in for the Fabric lakehouse tables (for the migrated assets) and for the legacy
assets' own storage; deterministic, seeded generation so counts don't drift between runs.

## Demo shape

**migration-both-states** (`CLAUDE.md`, "Migration prospects: show current AND future state"),
not `orchestrate-existing-workloads` applied only through metadata labels. SFS is mid-migration and
that's the actual point: the demo has to show the SSIS/homegrown-scheduler estate they're leaving
**and** the Fabric estate they're building, in one asset graph, with real lineage crossing between
them — `dim_dealer` and `dim_borrower` (fully Fabric-migrated) each depend on an asset that is
*still legacy today* (`raw_dealer_floorplan_feed`, `raw_credit_bureau_pull`). Those two legacy
assets are never triggered by Dagster — no mocked Fabric API call stands behind them — they are
observed only, via a sensor over SFS's own (mocked) scheduler run log, because SFS's own scheduler
is genuinely still the system of record for them.

This is a stricter bar than the coexistence pattern applied as metadata (`CLAUDE.md`, "Coexistence:
name the direction," pattern 4) that the previous build shipped: pattern 4 is still the right label
for the *relationship* (SFS's scheduler stays master for legacy workloads, Dagster owns the new
estate, lineage spans both) — keep `integration_pattern: "coexistence"` on the legacy assets — but
the previous build never actually built a second, Dagster-independent execution path, so
"coexistence" was true only in a string field. This rewrite is what makes it true in the graph.

The talk track: *"Half your portfolio's data is still coming from the system you're leaving. It's
already in the same lineage graph as the half you've moved, with the same freshness tracking, the
same checks, the same dashboard. You don't have to finish this migration to get value from
Dagster, and you don't have to route everything through us on day one."*

## Native integrations to use

- **No `dagster-dbt`.** Nothing in the AE notes or public research mentions dbt. Do not impose it.
- **No custom alerting sensor.** The AE notes name Teams/email as their notification channel.
  Dagster+ ships native alert policies for Slack, Teams, email, and PagerDuty covering run
  failures, asset check failures, freshness violations, and schedule/sensor failures — a *better*
  answer to "Teams/email notifications" than custom code. Point at the Dagster+ alert policy UI
  and talk through wiring it to their existing Teams channel. Do not write a custom sensor or
  hook for this.
- No native `dagster-fabric` package exists — Microsoft Fabric coverage lives entirely in the
  community registry. That makes the migrated side of this build a rung-2/rung-3 registry job, not
  a native-integration one. The legacy side has no vendor at all (it's a bespoke, in-house
  scheduler) — see Component strategy for how to treat that.

## Component strategy — read this before searching

**This is the section that caused the last four rebuilds. This pass's gap was narrower than the
prior ones but still rebuild-causing: the Fabric-side subclass and sensor were built correctly, but
nothing in the project gave the legacy side its own execution boundary. Fix that specifically —
don't re-litigate the parts that already work.**

**Fabric side (15 migrated assets) — unchanged from the fourth pass, keep this as-is:** subclass
`fabric_workspace` (rung 3) as **one component instance** covering all 15 items via the explicit
mapping table in `defs.yaml` (`assets_by_item_name`, the same shape as `assets_by_task_key` in the
Databricks workspace component,
`github.com/eric-thomas-dagster/databricks-workspace-bundles-demo`,
`defs/workspace_us/defs.yaml` — read that file before touching the subclass). Demo-mode discovery
returns the fixed 15-item list instead of hitting a live workspace; the `date` × `dealer_group`
partition config is added in the subclass and passed through. Keep `generate_sensor: true` on this
component — it still has a job: an already-migrated Fabric pipeline occasionally triggered by an
operator by hand, or by SFS's scheduler calling the Fabric API directly for a package mid-cutover,
is a real, separate, smaller coexistence story from the legacy-asset one below. Don't remove it;
just don't let it stand in for the real legacy boundary anymore.

**Legacy side (2 assets, `raw_dealer_floorplan_feed`, `raw_credit_bureau_pull`) — new this pass, and
the actual fix:** these are **not** entries in the `fabric_workspace` mapping table. There is no
Fabric pipeline behind them, mocked or real — SFS's own scheduler runs the SSIS package and Dagster
never calls anything to make it happen. Model them as:

- Plain `dg.AssetSpec` (or the component-form equivalent) declaring the asset with no Dagster-owned
  computation — an **external asset**, the same pattern `CLAUDE.md`'s "Orchestrating existing
  workloads" section describes for things Dagster didn't create.
- A **dedicated sensor** — separate from the Fabric workspace component's own polling sensor —
  polling a mocked legacy run-log source (extend or replace `external_run_history.py`'s approach:
  deterministic, seeded, static list of completions, not a live query) and emitting
  `AssetObservation` events with the arrival timestamp the lateness check needs.
- **Search the registry before writing this by hand.** SFS's scheduler is bespoke and in-house, so
  a vendor-specific component is unlikely, but check anyway with `--json`: `"external asset"`,
  `"observable source asset"`, `"observation sensor"`, `"generic polling sensor"`, `"sql server
  change tracking"`, `"database poller"`, `"file arrival sensor"`. If nothing fits — plausible, and
  fine — note the search trail. **A plain `AssetSpec` + a hand-written `@sensor` is core Dagster
  capability, not a "custom component"** in the rung-4 sense; it doesn't need a
  `component-feedback/` entry unless something more specific was actually searched for and found
  wanting (e.g. a generic database-poller component that almost fit but couldn't).

Keep `legacy_system_boundary: "homegrown_scheduler_owned"` and `integration_pattern: "coexistence"`
metadata on these two assets — now it describes what's actually happening, not just a label.

**If, after actually attempting this, some piece of the shape genuinely cannot be made to fit, that's
the moment for more custom code** — but the shape itself (external asset + observation sensor, no
Dagster-owned execution) is simple enough that this is unlikely to be the failure mode this time.

## Community components to search for

Search terms (not IDs — the build routine searches the registry itself), Fabric side (unchanged,
already covered adequately in prior passes — don't restart this search): "microsoft fabric",
"fabric pipeline trigger", "fabric lakehouse", "onelake", "fabric workspace", "power bi refresh".
LEARNINGS.md confirms registry coverage: `fabric_workspace`, `fabric_pipeline_trigger_job`,
`fabric_lakehouse_resource`, `fabric_lakehouse_io_manager`, `dataframe_to_fabric_lakehouse`,
`fabric_capacity_admin_job`, plus ~66 Azure and ~18 Databricks components.

Legacy side (new — search before writing the sensor by hand): "external asset", "observable source
asset", "observation sensor", "generic polling sensor", "sql server change tracking", "database
poller", "file arrival sensor". Record what was tried, even if the answer is "nothing fits, use
core Dagster `AssetSpec` + `@sensor`."

## Asset checks

At least these three, each mapped to a named pain. **All pass in the demo** — they're built,
wired, and visible so the prospect can see what they assert and where the result surfaces; they
don't need to see one go red to understand it:

1. **Blocking** — `raw_loan_originations` required-field completeness (loan_id, amount, dealer_id
   non-null) before the `stg_loan_originations` Fabric pipeline is triggered. Maps to: *"failure
   recovery is manual, replay is weak"* — the talk track is that bad data structurally cannot
   silently propagate into a downstream pipeline trigger, which is provable by reading the check
   config without ever making it fail.
2. **Blocking** — `abs_pool_eligibility` completeness/reconciliation check before the asset is
   considered eligible for securitization pool reporting. Maps to: the 2026 ABS securitization
   calendar and investor/rating-agency scrutiny of loan-tape accuracy.
3. **Warning** — `raw_dealer_floorplan_feed` arrival-time/lateness check, evaluated against the
   observation sensor's reported arrival timestamp (this asset is legacy/observed, not
   Dagster-triggered — the check still runs, it just evaluates data Dagster didn't produce). Maps
   to: *"asset-level expected timing / lateness visibility and downstream impact awareness"* — and
   is the sharper version of that story, since it's checking a feed still owned by the system
   they're trying to get visibility into.
4. (Optional if time allows) bronze-to-silver row-count reconciliation on payment transactions,
   mapped to *"UI/visibility is fragmented... hard to trust outputs."*

## Demo mode

- **What must be mocked:** On the Fabric side, the Fabric pipeline trigger/poll/discovery/observe
  API — in demo mode, the subclassed `fabric_workspace` component's discovery call returns the
  fixed 15-item list from the mapping table instead of hitting a live workspace, "triggering a
  Fabric pipeline" runs a local synthetic-data function with the same run/poll/complete lifecycle a
  real trigger-and-observe call would have, and its polling sensor polls a mocked run-history list
  for the occasional operator/legacy-scheduler-triggered Fabric run. On the legacy side, there is no
  API to mock in the usual sense — the two legacy assets are never triggered by Dagster at all;
  what's mocked is SFS's own scheduler's run log, which the dedicated legacy sensor polls to learn
  that a package completed and emit an `AssetObservation`. This is the network boundary to fake,
  per `templates/demo_mode_pattern.py`; asset keys, partitions, checks, sensors, and YAML schema
  stay identical between demo and real mode on both sides.
- **What can be real:** DuckDB standing in for the lakehouse tables the Fabric pipelines write to
  and for the legacy assets' own storage, the trigger-and-observe run lifecycle logic itself, all
  asset checks, freshness policies, and automation conditions.
- **Asset kinds to display:** `fabric` and optionally `azure` (max 3 kinds) on every Fabric-migrated
  bronze/silver/gold asset — there is no dbt manifest in this build, so no translator override is
  needed. On the two legacy assets, use a kind that reads as their actual system, e.g. `azure` alone
  or a plain descriptive kind for the SQL Server/SSIS layer — do not badge them `fabric`, since no
  Fabric pipeline is involved. `powerbi` on the reporting asset. Do not badge anything `duckdb`.
- **Everything materializes green.** Do not spec a planted anomaly, a corrupted partition, or any
  failure scenario, for any asset, ever — not the bronze completeness check, not the
  dealer-floorplan lateness check, not `abs_pool_eligibility`, not the legacy observation sensor.
  Checks, freshness policies, and the Dagster+ alert policies are built and visible, and Eric talks
  through what happens when they fire in production — that talk track goes in Capability talk track
  below, not into a live break/fix.
- **Data realism notes:** Loan/lease amounts, dealer counts, and delinquency rates should look
  like a mid-size captive auto lender — hundreds of dealers, thousands of contracts/day is a
  reasonable order of magnitude given the ~35-person data team. Treat exact figures as
  illustrative, not sourced. Deterministic, seeded generation — no drift between runs, so there is
  nothing to reset between demo audiences.

## Three buckets

- **In code:** The 17-asset graph — 15 Fabric-migrated trigger-and-observe assets plus 2 genuinely
  legacy external assets observed (never triggered) by Dagster via a dedicated sensor over SFS's own
  mocked scheduler run log; `date` × `dealer_group` partitions; the blocking and warning asset
  checks above (including one evaluated against externally-observed legacy data); freshness policies
  on `fact_delinquency_snapshot` and `abs_pool_eligibility`; `AutomationCondition.eager()` on the
  Fabric-triggered assets that should recompute themselves when a corrected vendor file lands; the
  subclassed `fabric_workspace` component with its own polling/observation sensor for the migrated
  side; dynamic metadata passing between pipeline steps (e.g. vendor file date), via Dagster's
  native asset metadata rather than a bespoke payload mechanism.
- **Handled by Dagster+ (demonstrate, don't build):** Teams/email alerting on run failures, check
  failures, and freshness violations, via native alert policies — never a custom sensor; restart /
  re-run from point of failure; asset lineage visualization; asset health, run history, and
  duration-trend views; the backfill UI, as the platform surface behind the partition-scoped
  replay story.
- **Conversation only (mention, build nothing):** AI-assisted operations, agentic monitoring, and
  "potentially self-healing workflows" — the AE notes' own phrase for a longer-term interest, not
  something this build stages or codes; a real embedded Power BI report or workspace integration
  (represented here as a single refresh-trigger asset only); the other ~683 SSIS packages beyond the
  two represented as legacy assets — mention the scale, don't model each one; NDA/procurement
  process.

## Demo name

**"The 700th Package"** — the scaling story in one phrase: adding SFS's next migrated SSIS package
(their 700th, or their 30th under Dagster) is one more row in a mapping table, not new code — and
until it's added, that package's data still shows up in the same lineage graph today, from the
system it's actually running on.

## Money shot

Screen: the full legacy-plus-Fabric asset graph, entirely green, with `fabric`/`azure`/`powerbi`
kind badges on the migrated side reading as their actual stack and a visibly different kind on the
two legacy assets. Click `dim_dealer` (fully Fabric-migrated) and show its one upstream dependency,
`raw_dealer_floorplan_feed` — its materialization history is entirely `AssetObservation` events from
the legacy sensor, never a Dagster-triggered run, because SFS's own scheduler is still the system of
record for that package today. Narrate: *"This asset is 100% built and running on Fabric. Its input
is still coming from the system you're leaving. Both are already in the same graph, right now, not
after the migration finishes."* Repeat briefly for `dim_borrower` / `raw_credit_bureau_pull`.

Then, live: rematerialize a single `raw_dealer_floorplan_feed`-downstream partition on the Fabric
side (one region, today's date) — it completes in seconds, and the eager automation condition on its
siblings shows what would recompute downstream, for that one region only. Narrate that this is what
"replay/backfill" looks like day to day — targeted, fast, and it never touches the other three
regions or the other 699 packages' worth of pipeline. Click into `abs_pool_eligibility`'s check
results — reconciliation check green — and narrate what it asserts and that it would block the
downstream pipeline trigger on failure. Finally, open the `defs.yaml` mapping table for the migrated
side and show that adding SFS's 30th (or 700th) pipeline is one more entry, not a new Python class —
and when that package moves, its asset just gains a second, Dagster-triggered execution path; nothing
about its identity in the graph changes.

## Capability talk track

- **Migration-both-states lineage:** *"Half your portfolio's data is still coming from the system
  you're leaving today, and it's already showing up in the same lineage graph, with the same
  freshness tracking, as the half you've already moved. You don't have to finish this migration to
  get value from Dagster."*
- **Asset checks (blocking):** *"This is your failure-recovery problem solved structurally — bad
  data physically cannot reach the ABS pool calculation, because the check gates the Fabric
  pipeline trigger before anything downstream runs. You don't have to watch it fail to believe
  that — the config is right here."*
- **Freshness policies:** *"This is your SLA tracking and lateness visibility — no custom
  dashboard needed, it's declared once on the asset, and it's what pages someone when
  `fact_delinquency_snapshot` goes stale — whether the upstream data came from Fabric or from the
  system you're leaving."*
- **Partition-scoped rematerialization:** *"This is your replay/backfill answer — you just watched
  us target one dealer region's one day without touching the other three regions or the other 699
  packages' worth of pipeline."*
- **Automation conditions (`AutomationCondition.eager()`):** *"This is what happens automatically
  when a corrected or new vendor file lands — you don't have to remember to re-trigger anything."*
- **Legacy observation sensor:** *"Nothing about this asset is faked or delayed — the moment your
  own scheduler finishes that package, Dagster knows, and it's in the same graph as everything
  else. That's true on day one of the migration and it's still true on day 699."*
- **Fabric-side polling sensor / observation:** *"And separately — even for a package that's already
  moved to Fabric, if someone runs it by hand or your own scheduler still calls the Fabric API
  directly mid-cutover, that shows up too. You're not locked into routing everything through us."*
- **Dagster+ native alert policies:** *"This routes into the same Teams channel your team already
  lives in, for run failures, check failures, and freshness violations — no custom sensor code to
  maintain, which is one less thing your team owns."*

## Explicitly out of scope

Rewriting the SSIS/stored-procedure transformation logic itself in a new engine — Dagster
orchestrates, tracks, and gates what Fabric already runs; it does not replace the transformation
logic inside those pipelines, migrated or not. Modeling all ~700 SSIS packages individually — two
representative legacy assets crossing into the Fabric estate make the migration-both-states point;
the rest are a narrative/README detail, not new nodes in the graph. Power BI is represented as a
single refresh-trigger asset, not a real embedded report — building an actual Power BI workspace
integration is out of scope for a one-night build. Multi-tenant RBAC/SSO and branch deployments are
not part of this brief. Credit-bureau data realism is illustrative only — no real bureau schemas or
scoring logic. Do not build anything resembling GLBA compliance tooling itself; the demo shows data
quality and lineage, not a compliance product. Do not stage any failure, anomaly, or recovery
scenario — see Demo mode.
