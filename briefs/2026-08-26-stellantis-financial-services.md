---
company: "Stellantis Financial Services"
slug: "stellantis-financial-services"
domain: "stellantis-fs.com"
demo_date: "TBD"        # AE doc: "Demo is being scheduled. Hopefully later the week of Aug 24th" — still no meeting on calendar as of this rewrite (checked 2026-08-26)
demo_time: ""
attendees:
  - "Nikitas (Nick) Gogos — Enterprise Data Management lead (new to company, ~3 months) — email unknown, found via LinkedIn Sales Navigator"
  - "Chris Rodriguez — leads enterprise data platform effort / software development leader — email unknown, found via LinkedIn Sales Navigator"
ae_doc: "https://docs.google.com/document/d/1NDlJt9BdhrnwfN6u3j3SRQ3F0zOvjAWJIPqi-YUKcuE/edit"
ae_doc_modified: "2026-08-24T14:57:11.995Z"
overall_confidence: "medium"
generated: "2026-08-26T21:50:00Z"
---

# Stellantis Financial Services — demo brief

<!-- REWRITE 2026-08-26 (fourth pass): Eric deployed the third rewrite's build
(PR #10, "Fourth build" per the ledger note — subclassing the registry's
`fabric_workspace` component as this brief's third pass mandated) and then
cleared both the brief and the project again the same evening ("Clear
stellantis brief and project, requeue", commit 2693658) — in the same sitting
where he landed two new CLAUDE.md sections, "Observe, don't just execute" and
"The workspace-component convention" (commit 4348277, 17:14, thirty minutes
before the deploy's own ledger timestamp of 16:50 had even finished being
reviewed).

PR #10's `DemoFabricWorkspaceComponent` got Rung 3 right — one subclassed
instance, one `defs.yaml` mapping table, partitions and deps carried through,
no rung-4 jump. But its `defs.yaml` never set `polling_sensor` (alias
`generate_sensor`), which every workspace-style component defaults to
**False**. Confirmed by reading the deleted build directly (`git show
fe5f718:...fabric_pipelines/defs.yaml` and `...fabric_workspace_demo.py`) —
neither file contains the string "sensor" anywhere. That means the demo only
ever showed runs Dagster itself triggered. For a demo whose own AE notes
describe an *evaluate whether Dagster can sit on top of (or replace) our
homegrown orchestrator* situation — where SFS's own scheduler is very much
still going to fire some of these ~700 packages during the migration — "what
happens when it wasn't Dagster that started it" isn't a nice-to-have, it's
close to the central question. Shipping without an answer to it is the gap
this rewrite exists to close.

Everything else is unchanged: research re-checked again 2026-08-26 (same AE
doc, `ae_doc_modified` unchanged, no calendar event found on a fresh search,
no new Drive material); the demo thesis, asset graph, escalation ladder, and
all-green demo mode all hold. This rewrite adds one non-negotiable build
directive — turn the polling sensor on and use it live — and brings the brief
up to the current `_TEMPLATE.md` shape, which gained "Fidelity", "Three
buckets", and "Demo name" sections (commit a67d0d9, filed the same evening)
after this brief's third pass was written. Nothing about those three sections
is a build-directive change; they make explicit choices that were already
implicit in the third pass. -->

## Demo thesis

Stellantis Financial Services' Enterprise Data Management lead (Nick, 3 months in role) is
racing to migrate ~700 homegrown SSIS packages onto Microsoft Fabric while the team triples in
size, and their single biggest complaint about the system they built themselves is that failure
recovery is manual and replay is weak — with SFS now underwriting up to eight auto ABS
securitization deals in 2026, an audit-ready, loan-level data tape is not optional. They are
evaluating Dagster as the orchestration layer *on top of, or replacing*, their homegrown system —
not asking for a new transformation stack: their bronze/silver/gold logic already exists, inside
the SSIS packages and stored procedures they're migrating into Fabric pipelines. This demo has to
prove that Dagster can wrap their *actual* Fabric pipelines with lineage, blocking checks,
freshness, and partition-scoped execution — every partition green, every check passing — and that
"replay is weak" and "recovery is manual" are answered by what the platform *shows*, not by a
staged break-and-fix. It also has to prove Dagster doesn't require an all-or-nothing cutover: a
pipeline triggered by their own scheduler mid-migration still shows up in the same lineage graph.
If Nick and Chris leave believing "Dagster sits on what we're already building in Fabric, gives us
the visibility our homegrown layer can't, and doesn't ask us to rebuild it — or to start everything
through Dagster on day one," the demo has done its job.

## Meeting

- **When:** Not booked yet — AE (Austin) doc says "hopefully later the week of Aug 24th." No
  calendar event found on a fresh search as of this rewrite (checked a fourth time, 2026-08-26 —
  same as all three prior passes). Intro call already happened Aug 21, 2026 (Gong-recorded).
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
partition-scoped operations their homegrown layer lacks.

## Current stack — confidence: high (source: AE discovery notes, corroborated by public job postings)

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Homegrown config-driven layer (evaluating Dagster vs. Airflow/Astronomer conceptually) | AE notes |
| Warehouse / platform | Microsoft Fabric (target), legacy Azure Synapse (prior gen) | AE notes |
| Transformation | SQL Server stored procedures / SSIS (~700 packages), bronze/silver/gold medallion pattern — **no dbt mentioned anywhere, in AE notes or in public research re-checked 2026-08-26** | AE notes |
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
  2026-08-26: no posting for either team mentions dbt, Airflow, or any named transformation
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
  Dagster's asset-centric pitch is built for), an active platform migration (Fabric), and a team
  that tripled in size — all consistent with a genuine re-platforming budget rather than a
  kick-the-tires eval.

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
- **No meeting booked yet** — re-checked calendar and Drive again for this rewrite (2026-08-26),
  still nothing scheduled and no new AE material beyond the same Aug 24 doc (unchanged since all
  three prior passes). The AE notes mention a "wish list" the prospect offered to share; it still
  hasn't shown up in Drive.
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
lease servicing feeding ABS pool reporting — as **their existing Fabric pipelines**, not a new
transformation layer. Partition on a `date` (daily) × `dealer_group` (a handful of regional dealer
groups, e.g. `midwest`, `northeast`, `south`, `west`) `MultiPartitionsDefinition` for the one asset
where a second dimension is genuinely part of their domain — dealer floorplan feeds arrive
per-region — so partition-scoped, targeted-reprocessing operations are demonstrable live without
needing anything to be broken first.

Every asset below is a **trigger-and-observe wrapper around a Fabric pipeline run** — standing in
for one of the ~700 SSIS packages being migrated. Dagster is not recomputing their transformation
logic in a new engine; it triggers, tracks, and gates pipelines that already exist (or are
actively being migrated into Fabric).

**Bronze (vendor ingestion, one Fabric-pipeline-triggered asset per feed):**
- `raw_loan_originations` — daily
- `raw_lease_originations` — daily
- `raw_payment_transactions` — daily
- `raw_dealer_floorplan_feed` — `date` × `dealer_group` (4 regions) — the one asset with a genuine
  second partition dimension
- `raw_credit_bureau_pull` — daily

**Silver (Fabric-pipeline-triggered conforming/staging — represents the existing SSIS/stored-proc
logic, now running as Fabric pipelines):**
- `stg_loan_originations`
- `stg_lease_originations`
- `stg_payment_transactions`
- `stg_delinquency_events`
- `dim_dealer` — rolls up all 4 `dealer_group` partitions for a date via a
  multi-to-single-dimension partition mapping
- `dim_borrower`

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

17 assets total, within the 12–25 target, spanning vendor ingestion → Fabric pipeline lineage →
BI — all as pipelines they already run or are actively migrating, not a rewrite.

## Fidelity

**Data-backed**, not graph-first. This demo needs real values on screen: the reconciliation and
completeness checks must compute their pass/fail from actual synthesized rows (not always pass by
construction the way a `pass`-bodied asset would), the dealer-floorplan lateness check needs a real
arrival timestamp to compare against, and the money-shot's ABS-pool and delinquency numbers need to
look like a real loan portfolio when Nick and Chris ask "how many contracts is that." Back it with
DuckDB standing in for the Fabric lakehouse tables; deterministic, seeded generation so counts don't
drift between runs.

## Demo shape

**orchestrate-existing-workloads**, specifically the **coexistence** pattern (`CLAUDE.md`,
"Coexistence: name the direction," pattern 4): SFS's own homegrown scheduler stays master for
legacy SSIS packages still being migrated, Dagster owns orchestration for what's already moved to
Fabric, and lineage spans both during the transition. This is the demo shape the AE's own words
describe — "evaluate whether Dagster can become the production orchestration layer on top of (or
replacing) that homegrown system" is additive orchestration over an existing estate, not a request
to rebuild their bronze/silver/gold logic in a new tool. Reach for trigger-and-observe components
over the Fabric pipelines, not `dagster-dbt` or any from-scratch transformation layer. Tag the
component and its assets `integration_pattern: "coexistence"` per the CLAUDE.md convention, and
make the boundary explicit with `legacy_system_boundary` metadata naming which packages are still
homegrown-scheduler-owned vs. migrated.

## Native integrations to use

- **No `dagster-dbt`.** Nothing in the AE notes or public research mentions dbt. Do not impose it.
- **No custom alerting sensor.** The AE notes name Teams/email as their notification channel.
  Dagster+ ships native alert policies for Slack, Teams, email, and PagerDuty covering run
  failures, asset check failures, freshness violations, and schedule/sensor failures — a *better*
  answer to "Teams/email notifications" than custom code. Point at the Dagster+ alert policy UI
  and talk through wiring it to their existing Teams channel. Do not write a custom sensor or
  hook for this.
- No native `dagster-fabric` package exists — Microsoft Fabric coverage lives entirely in the
  community registry. That makes this a rung-2/rung-3 build, not a native-integration one.

## Component strategy — read this before searching

**This is the section that caused the last three rebuilds. Get it right this time.**

The 2026-08-26 second build (PR #9) rejected the registry's `fabric_workspace` component and wrote
a custom `FabricPipelineAssetComponent` instead for three reasons — "it discovers items from a live
connection," "asset keys come from the source system, not us," "no partitions support" — that
`CLAUDE.md`'s Rung 3 table now names explicitly as NOT disqualifying. The third build (PR #10)
fixed that: it subclassed `fabric_workspace` correctly, as one instance with an explicit mapping
table. **That build's remaining gap was narrower but still a rebuild-causing one: it never turned
the component's polling sensor on.** Every workspace-style component — `fabric_workspace` included
— ships a `polling_sensor` field (alias `generate_sensor`), **default `False`**. PR #10's
`defs.yaml` left it unset, so the demo only ever showed Dagster-triggered runs, with no answer to
"what happens when it wasn't Dagster that started it" — which for a coexistence-pattern demo, where
SFS's own scheduler is still triggering some of these packages during the migration, is close to
the whole point.

Subclass `fabric_workspace` (rung 3) as **one component instance** covering all ~17 items via the
explicit mapping table in `defs.yaml`:

- *Live discovery* → subclass, override the discovery/list call, return a fixed item list in demo
  mode (the same demo_mode seam every other component in this repo uses — see
  `templates/demo_mode_pattern.py`).
- *Asset keys from the source system* → an `assets_by_item_name`-style mapping field binding each
  of the ~17 Fabric pipeline items to the asset key, deps, partitions, and metadata this brief
  specifies — the same shape as `assets_by_task_key` in the Databricks workspace component
  (`github.com/eric-thomas-dagster/databricks-workspace-bundles-demo`,
  `defs/workspace_us/defs.yaml`). Read that file before writing the subclass.
- *No partitions support* → add the `date` × `dealer_group` partitions config in the subclass and
  pass it through to the specs the parent builds.
- **Turn the polling sensor on.** Set it explicitly in `defs.yaml`:

  ```yaml
  attributes:
    generate_sensor: true
  ```

  In demo mode, the subclass's sensor should poll the same mocked run-history list the
  discovery/trigger calls use, and surface at least one Fabric pipeline run per demo session that
  Dagster did not start — representing a package still triggered by SFS's own homegrown scheduler,
  or an operator running one by hand — as an `AssetObservation`. This is not decorative: it is the
  literal, direct answer to the AE's own coexistence framing, and it's a required part of the money
  shot below, not an optional add-on.

**Do not repeat the rung-4 jump**, and do not repeat shipping without the sensor. Compose freshness
policies, retry policies, and automation conditions from outside the component via `defs.yaml`,
the same way `AssetSpec`-based components already do elsewhere in this repo — that's a
config/wiring concern, not a reason to reject the base component.

If, after actually attempting the subclass, some piece of the shape genuinely cannot be made to
fit (not "would take an extra field," but a real structural mismatch), that's the moment for a
custom component — built to the workspace-component convention (`@public get_asset_spec(props)`,
opt-in polling sensor, `StateBackedComponent` inheritance, a `translation:` field), never as a
one-off — and write the feedback file per `CLAUDE.md`'s "When you write a custom component, write
feedback" section, including the literal subclass attempt that didn't work, not just registry-search
terms.

The second custom component from the second build, `DuckDbAssetCheckComponent` (an arbitrary
blocking/warning SQL assertion against an asset materialized outside dbt), was **not** covered by
that run's feedback file at all — its search only covered Fabric terms. Before writing anything
custom for the asset-check layer, run at least three distinct registry searches for this
specifically (e.g. `"sql assertion check"`, `"data quality check"`, `"duckdb check"`,
`"custom check"`, `"row count check"`) with `--json`, and record what you tried and why it didn't
fit, even if the answer turns out to be "nothing fits, build it." Rung 4 requires the search
trail; a custom check component without one is unjustified per `CLAUDE.md`.

## Community components to search for

Search terms (not IDs — the build routine searches the registry itself): "microsoft fabric",
"fabric pipeline trigger", "fabric lakehouse", "onelake", "fabric workspace", "power bi refresh".
LEARNINGS.md confirms registry coverage: `fabric_workspace`, `fabric_pipeline_trigger_job`,
`fabric_lakehouse_resource`, `fabric_lakehouse_io_manager`, `dataframe_to_fabric_lakehouse`,
`fabric_capacity_admin_job`, plus ~66 Azure and ~18 Databricks components. Start from
`fabric_workspace` and subclass it per the Component strategy section above — do not restart the
registry search from scratch, prior runs' searches already covered this domain adequately; the gap
was in the escalation decision and the sensor config, not the search.

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
3. **Warning** — `raw_dealer_floorplan_feed` arrival-time/lateness check. Maps to: *"asset-level
   expected timing / lateness visibility and downstream impact awareness."*
4. (Optional if time allows) bronze-to-silver row-count reconciliation on payment transactions,
   mapped to *"UI/visibility is fragmented... hard to trust outputs."*

## Demo mode

- **What must be mocked:** The Fabric pipeline trigger/poll/discovery/observe API itself — in demo
  mode, the subclassed `fabric_workspace` component's discovery call returns the fixed ~17-item
  list from the mapping table instead of hitting a live workspace, "triggering a Fabric pipeline"
  runs a local synthetic-data function with the same run/poll/complete lifecycle a real
  trigger-and-observe call would have, and the polling sensor (turned on — see Component strategy)
  polls a mocked run-history list that includes at least one run Dagster didn't start. This is the
  network boundary to fake, per `templates/demo_mode_pattern.py`; the component class, asset keys,
  partitions, checks, sensor, and YAML schema stay identical between demo and real mode.
- **What can be real:** DuckDB standing in for the lakehouse tables the Fabric pipelines write to,
  the trigger-and-observe run lifecycle logic itself, all asset checks, freshness policies, and
  automation conditions.
- **Asset kinds to display:** `fabric` on every bronze/silver/gold asset (there is no dbt manifest
  in this build, so no translator override is needed), `azure` optionally alongside it if it reads
  cleanly (max 3 kinds), `powerbi` on the reporting asset. Do not badge anything `duckdb`.
- **Everything materializes green.** Do not spec a planted anomaly, a corrupted partition, or any
  failure scenario, for any asset, ever — not the bronze completeness check, not the
  dealer-floorplan lateness check, not `abs_pool_eligibility`. Checks, freshness policies, and the
  Dagster+ alert policies are built and visible, and Eric talks through what happens when they
  fire in production — that talk track goes in Capability talk track below, not into a live
  break/fix.
- **Data realism notes:** Loan/lease amounts, dealer counts, and delinquency rates should look
  like a mid-size captive auto lender — hundreds of dealers, thousands of contracts/day is a
  reasonable order of magnitude given the ~35-person data team. Treat exact figures as
  illustrative, not sourced. Deterministic, seeded generation — no drift between runs, so there is
  nothing to reset between demo audiences.

## Three buckets

- **In code:** The 17-asset trigger-and-observe Fabric pipeline graph; `date` × `dealer_group`
  partitions; the blocking and warning asset checks above; freshness policies on
  `fact_delinquency_snapshot` and `abs_pool_eligibility`; `AutomationCondition.eager()` on the
  Fabric-triggered assets that should recompute themselves when a corrected vendor file lands; the
  subclassed `fabric_workspace` component with its polling/observation sensor turned on; dynamic
  metadata passing between pipeline steps (e.g. vendor file date), via Dagster's native asset
  metadata rather than a bespoke payload mechanism.
- **Handled by Dagster+ (demonstrate, don't build):** Teams/email alerting on run failures, check
  failures, and freshness violations, via native alert policies — never a custom sensor; restart /
  re-run from point of failure; asset lineage visualization; asset health, run history, and
  duration-trend views; the backfill UI, as the platform surface behind the partition-scoped
  replay story.
- **Conversation only (mention, build nothing):** AI-assisted operations, agentic monitoring, and
  "potentially self-healing workflows" — the AE notes' own phrase for a longer-term interest, not
  something this build stages or codes; a real embedded Power BI report or workspace integration
  (represented here as a single refresh-trigger asset only); NDA/procurement process.

## Demo name

**"The 700th Package"** — the scaling story in one phrase: adding SFS's next migrated SSIS package
(their 700th, or their 30th under Dagster) is one more row in a mapping table, not new code, and
every package — Dagster-triggered or homegrown-scheduler-triggered — lands in the same lineage
graph.

## Money shot

Screen: the full vendor-file → bronze → silver → gold Fabric pipeline graph, entirely green, with
`fabric`/`azure`/`powerbi` kind badges making it visually read as their actual stack — because it
*is* their stack, triggered and gated, not reimplemented. `abs_pool_eligibility` and
`fact_delinquency_snapshot` show freshness policies satisfied. Click into `abs_pool_eligibility`'s
check results — reconciliation check green — and narrate what it asserts and that it would block
the downstream pipeline trigger on failure. Then, live: rematerialize a single
`raw_dealer_floorplan_feed` partition (one region, today's date) — it completes in seconds, and the
eager automation condition on `stg_loan_originations`'s siblings shows what would recompute
downstream, for that one region only. Narrate that this is what "replay/backfill" looks like day
to day — targeted, fast, and it never touches the other three regions or the other 699 packages'
worth of pipeline, whether the trigger is a backfill, a schema change, or an operator's judgment
call, not a failure. Also worth 20 seconds: point at one asset whose latest event is an
`AssetObservation` from the polling sensor, not a Dagster-triggered run — proof that a package
still triggered by SFS's own scheduler, or run by hand mid-migration, shows up in the same lineage
graph with the same freshness tracking, which is the literal coexistence story Nick and Chris are
evaluating. Finally, open the `defs.yaml` mapping table itself and show that adding SFS's 30th (or
700th) pipeline is one more entry in that file, not a new Python class — directly answers "is this
bespoke per pipeline."

## Capability talk track

- **Asset checks (blocking):** *"This is your failure-recovery problem solved structurally — bad
  data physically cannot reach the ABS pool calculation, because the check gates the Fabric
  pipeline trigger before anything downstream runs. You don't have to watch it fail to believe
  that — the config is right here."*
- **Freshness policies:** *"This is your SLA tracking and lateness visibility — no custom
  dashboard needed, it's declared once on the asset, and it's what pages someone when
  `fact_delinquency_snapshot` goes stale."*
- **Partition-scoped rematerialization:** *"This is your replay/backfill answer — you just watched
  us target one dealer region's one day without touching the other three regions or the other 699
  packages' worth of pipeline. That's true whether you're backfilling a schema change or
  reprocessing after an upstream fix — the mechanism doesn't change."*
- **Automation conditions (`AutomationCondition.eager()`):** *"This is what happens automatically
  when a corrected or new vendor file lands — you don't have to remember to re-trigger anything."*
- **Polling sensor / observation:** *"This is what happens when it wasn't Dagster that kicked off
  the pipeline — your own scheduler, or someone running a package by hand during the migration,
  still shows up here with the same lineage and freshness tracking. You're not locked into routing
  everything through us on day one."*
- **Dagster+ native alert policies:** *"This routes into the same Teams channel your team already
  lives in, for run failures, check failures, and freshness violations — no custom sensor code to
  maintain, which is one less thing your team owns."*
- **Fabric pipeline lineage (trigger-and-observe, one component, explicit mapping):** *"This is
  lineage across the exact ~700 packages you're migrating into Fabric today — Dagster sits on top
  of what you're already building, it doesn't ask you to rebuild it. And your 700th package is one
  more row in this table, not a new class of code."*

## Explicitly out of scope

Rewriting the SSIS/stored-procedure transformation logic itself in a new engine — Dagster
orchestrates, tracks, and gates what Fabric already runs; it does not replace the transformation
logic inside those pipelines. Power BI is represented as a single refresh-trigger asset, not a
real embedded report — building an actual Power BI workspace integration is out of scope for a
one-night build. Multi-tenant RBAC/SSO and branch deployments are not part of this brief.
Credit-bureau data realism is illustrative only — no real bureau schemas or scoring logic. Do not
build anything resembling GLBA compliance tooling itself; the demo shows data quality and lineage,
not a compliance product. Do not stage any failure, anomaly, or recovery scenario — see Demo mode.
