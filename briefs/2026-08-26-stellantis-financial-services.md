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
generated: "2026-08-26T00:00:00Z"
---

# Stellantis Financial Services — demo brief

<!-- REWRITE 2026-08-26: Eric deleted the original 2026-08-25 brief and the
generated demo ("Clear stellantis brief and requeue" / "Delete
demos/stellantis-financial-services directory — need to rebuild"), in the
same session that landed the new CLAUDE.md rule "the feature floor is Dagster
capabilities, not a fixed toolchain" and the "orchestrating existing
workloads" demo shape. The original brief called for a from-scratch dbt
bronze/silver/gold project — but nothing in the AE notes or in public
research mentions dbt. SFS's bronze/silver/gold logic already lives in the
~700 SSIS packages and stored procedures they're migrating onto Microsoft
Fabric, and their explicit ask was to model *that* flow, not build a new
transformation stack next to it. This rewrite keeps the company research
(re-verified 2026-08-26 — nothing material changed) and replaces the build
directives with the orchestrate-existing-workloads shape: Dagster
trigger-and-observe over their actual Fabric pipelines, not a dbt project. -->

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
freshness, and partition-scoped recovery — rematerialize the one late vendor file, not the whole
700-job chain — without asking them to rewrite a single package. If Nick and Chris leave believing
"Dagster sits on what we're already building in Fabric, it doesn't ask us to rebuild it," the demo
has done its job.

## Meeting

- **When:** Not booked yet — AE (Austin) doc says "hopefully later the week of Aug 24th." No
  calendar event exists as of this rewrite (checked again 2026-08-26). Intro call already
  happened Aug 21, 2026 (Gong-recorded).
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
their vendor-file → bronze → silver → gold flow and show how an operator recovers from a failed
step without rerunning everything.

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
file date)."*

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
  in 2026. Re-checked 2026-08-26: parent Stellantis N.V. took analyst downgrades this week (UBS,
  Bernstein) on U.S. turnaround/tariff concerns and is weighing the Brampton plant's future — none
  of that is about the finance arm's data platform decision and shouldn't be read into the room,
  but it's worth knowing the parent is under cost pressure if budget scrutiny comes up.
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
- **No meeting booked yet** — same as the original brief; re-checked calendar 2026-08-26, still
  nothing scheduled.

---

# Build directives

## Asset graph

Model the vendor-file → bronze → silver → gold flow the AE notes explicitly ask for, themed on
auto-loan/lease servicing feeding ABS pool reporting — but as **their existing Fabric pipelines**,
not a new transformation layer. Partition on `MultiPartitionsDefinition` of `date` (daily) ×
`vendor_source` (a handful of feeds — e.g. `dealer_originations`, `servicer_payments`,
`credit_bureau`, `dealer_floorplan`) so the recovery story is genuinely partition-scoped, matching
"replay/backfill" as a named pain.

Every asset below is a **trigger-and-observe wrapper around a Fabric pipeline run** — standing in
for one of the ~700 SSIS packages being migrated. Dagster is not recomputing their transformation
logic in a new engine; it triggers, tracks, and gates pipelines that already exist (or are
actively being migrated into Fabric). This is the direct answer to "evaluating whether Dagster can
become the production orchestration layer on top of (or replacing) that homegrown system" — and it
is the whole reason this brief was rewritten (the prior version built these as dbt models, which
nothing in the AE notes or public research supports).

**Bronze (vendor ingestion, one Fabric-pipeline-triggered asset per feed, partitioned date × source):**
- `raw_loan_originations`
- `raw_lease_originations`
- `raw_payment_transactions`
- `raw_dealer_floorplan_feed` — this is the one feed with a genuine retry policy (see below)
- `raw_credit_bureau_pull`

**Silver (Fabric-pipeline-triggered conforming/staging — represents the existing SSIS/stored-proc
logic, now running as Fabric pipelines):**
- `stg_loan_originations`
- `stg_lease_originations`
- `stg_payment_transactions`
- `stg_delinquency_events`
- `dim_dealer`
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

~16 assets total, within the 12–25 target, spanning vendor ingestion → Fabric pipeline lineage →
BI — all as pipelines they already run or are actively migrating, not a rewrite.

## Demo shape

**orchestrate-existing-workloads.** This is the demo shape the AE's own words describe — "evaluate
whether Dagster can become the production orchestration layer on top of (or replacing) that
homegrown system" is additive orchestration over an existing estate, not a request to rebuild
their bronze/silver/gold logic in a new tool. Reach for trigger-and-observe components over the
Fabric pipelines, not `dagster-dbt` or any from-scratch transformation layer. (The original
2026-08-25 brief got this wrong — see the rewrite note at the top of this file.)

## Native integrations to use

- **No `dagster-dbt`.** Nothing in the AE notes or public research (job postings re-checked
  2026-08-26) mentions dbt. Do not impose it — this is the exact "confident-wrong-guess" failure
  the house rules now call out by name.
- `dagster-msteams` — SFS explicitly said Teams/email are how they'd want notifications; this is a
  native integration that maps directly to a stated requirement. Wire an alerting hook off the
  blocking check and the freshness policies.
- Microsoft Fabric trigger-and-observe components (see registry search below) for the bronze/
  silver/gold pipeline triggers and lakehouse IO. No native `dagster-fabric` package exists, so
  this is rung 2 of the component escalation ladder — registry, as-is.

## Community components to search for

Search terms (not IDs — the build routine searches the registry itself): "microsoft fabric",
"fabric pipeline trigger", "fabric lakehouse", "onelake", "fabric workspace", "power bi refresh",
"teams webhook notification", "email notification asset check". LEARNINGS.md (verified 2026-08-25)
already confirms registry coverage: `fabric_workspace`, `fabric_pipeline_trigger_job`,
`fabric_lakehouse_resource`, `fabric_lakehouse_io_manager`, `dataframe_to_fabric_lakehouse`,
`fabric_capacity_admin_job`, plus ~66 Azure and ~18 Databricks components. Use these directly
(rung 2); only subclass (rung 3) if a field genuinely doesn't fit SFS's shape.

## Asset checks

At least these three, each mapped to a named pain — checks operate on asset materializations
regardless of what triggered them, so this list is unchanged in substance from the original brief:

1. **Blocking** — `raw_loan_originations` required-field completeness (loan_id, amount, dealer_id
   non-null) before the `stg_loan_originations` Fabric pipeline is triggered. Maps to: *"failure
   recovery is manual, replay is weak"* — show that bad data structurally cannot silently
   propagate into a downstream pipeline trigger.
2. **Blocking** — `abs_pool_eligibility` completeness/reconciliation check before the asset is
   considered eligible for securitization pool reporting. Maps to: the 2026 ABS securitization
   calendar and investor/rating-agency scrutiny of loan-tape accuracy.
3. **Warning** — `raw_dealer_floorplan_feed` arrival-time/lateness check. Maps to: *"asset-level
   expected timing / lateness visibility and downstream impact awareness."*
4. (Optional if time allows) bronze-to-silver row-count reconciliation on payment transactions,
   mapped to *"UI/visibility is fragmented... hard to trust outputs."*

## Demo mode

- **What must be mocked:** The Fabric pipeline trigger/poll API itself — in demo mode,
  "triggering a Fabric pipeline" runs a local synthetic-data function with the same
  run/poll/complete lifecycle a real trigger-and-observe call would have (this is the network
  boundary to fake, per `templates/demo_mode_pattern.py`), plus the vendor file drops and Teams
  notifications (log/no-op the webhook call in demo mode).
- **What can be real:** DuckDB standing in for the lakehouse tables the Fabric pipelines write to,
  the trigger-and-observe run lifecycle logic itself, all asset checks, freshness policies, and
  automation conditions.
- **Asset kinds to display:** `fabric` on every bronze/silver/gold asset (from the Fabric
  lakehouse IO manager / trigger component — there is no dbt manifest in this build, so no
  translator override is needed this time), `azure` optionally alongside it if it reads cleanly
  (max 3 kinds), `powerbi` on the reporting asset. Do not badge anything `duckdb`.
- **Failure demonstration: yes.** Unchanged from the original brief — this is the one explicit,
  named ask from the AE's meeting-agenda notes: *"how operators recover from a failed step without
  rerunning everything."*
- **If yes — planted anomaly:** One `raw_dealer_floorplan_feed` partition (a specific `date` ×
  `dealer_floorplan` key) arrives with a malformed record (missing `loan_id`). The blocking check
  on that bronze asset fails and the downstream silver/gold Fabric pipeline triggers for that
  partition never fire. Recovery is: the mock vendor source is updated (simulating the dealer
  resending a corrected file), and Nick/Chris watch you rematerialize only that one partition —
  which now successfully triggers just that one Fabric pipeline run, not the other vendor-source
  partitions for that date, and not any other date, and not the other 699 packages. No heal asset,
  no reset job — the fix lives in `demo_data/`, exactly per house rules.
- **Demo reset:** A script or `make` target that restores the mock vendor-source state (both the
  "bad" and "corrected" versions of the flagged file) so the sequence can be re-run for a second
  audience.
- **Data realism notes:** Loan/lease amounts, dealer counts, and delinquency rates should look
  like a mid-size captive auto lender — hundreds of dealers, thousands of contracts/day is a
  reasonable order of magnitude given the ~35-person data team. Treat exact figures as
  illustrative, not sourced.

## Money shot

Screen: the full vendor-file → bronze → silver → gold Fabric pipeline graph, green, with
`abs_pool_eligibility` and `fact_delinquency_snapshot` showing freshness policies satisfied, and
Fabric/Power BI kind badges making it visually read as their actual stack — because it *is* their
stack, triggered and gated, not reimplemented. Then: select the one `raw_dealer_floorplan_feed`
partition with the planted anomaly, show the blocking check red and the downstream Fabric pipeline
trigger refusing to fire for that partition only, "fix" the mock vendor file, and rematerialize
just that partition — narrate that this is the entire recovery procedure, no custom tooling, no
reset step, no rerun of the other 699 packages' worth of work.

## Capability talk track

- **Asset checks (blocking):** *"This is your failure-recovery problem solved structurally — bad
  data physically cannot reach the ABS pool calculation, because the check gates the Fabric
  pipeline trigger before anything downstream runs."*
- **Freshness policies:** *"This is your SLA tracking and lateness visibility — no custom
  dashboard needed, it's declared once on the asset."*
- **Partition-scoped rematerialization:** *"This is your replay/backfill answer — you just watched
  us recover one dealer's one day of data without touching the other 699 packages' worth of
  pipeline."*
- **Automation conditions (`AutomationCondition.eager()`):** *"This is what happens automatically
  when a corrected vendor file lands — you don't have to remember to re-trigger anything."*
- **`dagster-msteams` alerting hook:** *"This posts to the same Teams channel your team already
  lives in — no new tool to check."*
- **Fabric pipeline lineage (trigger-and-observe):** *"This is lineage across the exact ~700
  packages you're migrating into Fabric today — Dagster sits on top of what you're already
  building, it doesn't ask you to rebuild it."*

## Explicitly out of scope

Rewriting the SSIS/stored-procedure transformation logic itself in a new engine — Dagster
orchestrates, tracks, and gates what Fabric already runs; it does not replace the transformation
logic inside those pipelines. Power BI is represented as a single refresh-trigger asset, not a
real embedded report — building an actual Power BI workspace integration is out of scope for a
one-night build. Multi-tenant RBAC/SSO and branch deployments are not part of this brief.
Credit-bureau data realism is illustrative only — no real bureau schemas or scoring logic. Do not
build anything resembling GLBA compliance tooling itself; the demo shows data quality and lineage,
not a compliance product.
