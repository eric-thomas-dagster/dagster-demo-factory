---
company: "Partners Federal Credit Union"
slug: "partners-fcu"
domain: "partnersfcu.org"
demo_date: "2026-09-10"
demo_time: "2:00–3:00 PM ET (1 hour)"
attendees: ["Mark Smith — Enterprise Architect, Partners FCU, 3.5 years, has used Airflow at a prior job — mark.smith@partnersfcu.org", "Jason Jasmin — BI Technical Lead, 6 years, ran the original Dagster OSS pilot, no Airflow background (AE notes give his surname as \"Chapman\" — likely a notes typo; calendar invite says \"Jasmin,\" same role/description, treated as one person, not independently re-confirmed) — jason.jasmin@partnersfcu.org", "Srinath Sesham — Data Architect, 9 years — srinath.sesham@partnersfcu.org", "Len — implementation consultant assisting Partners FCU, Blue Coconut Strategies — len@bluecoconutstrategies.com", "Naomi — Dagster AE (organizer) — naomi@dagsterlabs.com", "Eric Thomas — Dagster SE — eric.thomas@dagsterlabs.com"]
ae_doc: "https://docs.google.com/document/d/19A5tDtnG4xpD4DUzXyiuVcAxgElC9oc_-eFT5GDDg9k/edit"
ae_doc_modified: "2026-09-09"
overall_confidence: "medium"
generated: "2026-09-09"
---

# Partners Federal Credit Union — demo brief

## Demo thesis

Partners FCU already tried Dagster OSS about a year ago and let it lapse to
competing priorities, not to a technical objection — so this meeting is "prove
what's different now," not a first look. They're a SQL Server + dbt Core shop
that is, in their own words, "always building workarounds to get jobs to run
in the right order and have dependencies running properly," and they're
actively replatforming their BI architecture under a newly appointed SVP/Chief
Digital & Innovation Officer — an 18-year Disney technology veteran, and
exactly the kind of new-exec budget signal worth walking in with confidence
about. Their own name for this meeting is "Pro vs. OSS," and their ask is
concrete, not general: alerting (explicitly untested in the OSS pilot and the
thing they "really care about"), quality checks, and partitioned assets. This
demo has one job — answer "why pay for Dagster+ over what we already tried
and let stall" on exactly those three named items, specifically enough that
it doesn't stall a second time.

## Meeting

- **When:** 2026-09-10, 2:00–3:00 PM ET (1 hour).
- **Who's in the room:** Mark Smith (Enterprise Architect, 3.5 years, has used
  Airflow elsewhere), Jason Jasmin (BI Technical Lead, 6 years, ran the
  original OSS pilot, no Airflow background — the main point of contact),
  Srinath Sesham (Data Architect, 9 years), and Len (implementation
  consultant from Blue Coconut Strategies, assisting Partners FCU).
- **Meeting type:** Technical deep-dive / competitive comparison
  (Dagster+ vs. self-hosted OSS) — a re-engagement after a stalled pilot,
  not a first-look demo.

## Use case — confidence: high

Partners FCU is replatforming its BI architecture off brittle, hand-sequenced
SQL Server jobs. They already run dbt Core against SQL Server and want
Dagster to own orchestration; Snowflake is a likely near-term warehouse
addition. They piloted Dagster OSS roughly a year ago and let it stall for
reasons unrelated to the product ("too much else going on"), and are now
specifically evaluating Dagster+ over self-hosting, with alerting named as
the deciding feature. (AE notes, direct.)

## Current stack — confidence: medium (SQL Server and dbt are high; everything else is lower)

| Layer | Tool | Source |
|---|---|---|
| Orchestration | None today — hand-sequenced SQL Server jobs with manual dependency workarounds; evaluating Dagster+ vs. self-hosted Dagster OSS | AE notes — direct |
| Warehouse | SQL Server today; Snowflake a likely near-term addition | AE notes — direct ("likely also adding Snowflake") |
| Transformation | dbt Core, running against SQL Server today | AE notes — direct |
| Ingestion | Not named | unknown |
| BI | Not named (only "BI architecture" broadly) | unknown |
| Cloud | Possible Azure footprint alongside self-hosted SQL Server | Public job posting (medium confidence, see below) — not mentioned in AE notes |
| CI/CD | Not named | unknown |

**Public cross-check:** the one open, public Partners FCU req found — a
Database Administrator role — names **SQL Server (self-hosted), SQL Server
Managed Instance, Azure SQL, Managed PostgreSQL, and InfluxDB** in scope
(medium confidence: WebSearch snippet only, full posting text not
independently fetched — egress to the ATS domain was blocked). This suggests
a more Azure-hybrid footprint than "pure on-prem SQL Server shop," worth
confirming live rather than assuming. No data engineer, analytics engineer,
or BI developer roles are currently public for Partners FCU on any job board
searched — the stack-modernization signal here is coming entirely from the
AE conversation, not from public hiring, at least not right now. No public
mention anywhere of Snowflake, Databricks, Airflow, Power BI, Tableau, SSIS,
or Fivetran tied to Partners FCU specifically — silence here doesn't
contradict the AE notes (Snowflake is explicitly forward-looking, so it
wouldn't show up in a current job post), it just isn't independent
corroboration either.

## Pain — confidence: high

Direct from AE notes:

- **"Always building workarounds to get jobs to run in the right order and
  have dependencies running properly"** — classic ad hoc job-scheduler
  dependency management, the direct analog to Dagster's declarative asset
  graph.
- **Alerting is untested and is the named decision criterion** — "they
  *really* care about alerting and + features." The OSS pilot didn't answer
  this; this meeting exists specifically to.
- **The stalled prior POC is itself a pain signal.** OSS wasn't rejected on
  technical grounds — it lost to competing priorities. This demo needs to
  remove the "not complete/managed enough to stick" objection, which is a
  different job than a first-look pitch.

## Data domains

Not named in the AE notes — no specific tables or subject areas given. The
build should use generic, recognizable credit-union nouns (member
transactions, deposit accounts, loan originations) rather than invent
specifics, and this is flagged explicitly as an **assumption**, not a
confirmed table list.

**Scale/cadence:** No volumes or SLAs given. Partners FCU is a mid-sized
credit union (~165,000 members per its public profile, 250–499 employees per
public estimates) — daily-cadence core-banking/BI batch feeds is a reasonable
default; nothing suggests high-frequency streaming. Also an assumption.

## Public signals

- **Job postings:** One open, public req — Database Administrator — naming
  SQL Server (self-hosted), SQL Server Managed Instance, Azure SQL, Managed
  PostgreSQL, and InfluxDB (medium confidence, snippet-only). No open data
  engineer, analytics engineer, or BI developer roles found on any job board
  searched as of today.
- **Engineering blog / GitHub:** None found — expected for a member-only
  credit union with no public engineering presence; not treated as a
  negative signal.
- **Recent news:** **Darla Morse appointed SVP & Chief Digital and
  Innovation Officer**, explicitly covering digital, technology, and
  innovation strategy — 18 years at The Walt Disney Company (VP, Segment
  Global Business Technology) before Red Robin (EVP/CIO), now at Partners
  FCU (source: partnersfcu.org). A new C-suite technology exec landing right
  as they're "replatforming BI architecture" is exactly the re-platforming
  budget signal these house rules call out. Also: Brian Kairnes promoted to
  EVP, Chief Risk & Lending Officer (March 2026) — leadership churn, not
  directly data-relevant. A 3rd-consecutive-year Newsweek "America's Best
  Credit Unions" recognition is a brand fact only.
- **Industry / compliance:** NCUA-regulated (not FDIC). The GLBA Safeguards
  Rule (NCUA Part 748) requires documented vendor due diligence and
  technical safeguards for any third party touching member financial data,
  and NCUA's 2026 supervisory priorities specifically sharpen third-party/
  vendor-risk-management expectations. This is a genuinely usable framing,
  not decoration: Dagster's lineage, asset checks, and observability are
  evidence a credit union can hand to an examiner or its own vendor-risk
  committee, on top of being an engineering capability.
- **Orchestration signals:** No public trace of the year-old OSS pilot
  (expected — internal engagement, no public footprint). No public
  confirmation of Snowflake, Databricks, Airflow, Power BI, Tableau, SSIS,
  or Fivetran specifically tied to Partners FCU.

## Conflicts and gaps

- **Surname conflict:** AE notes name the BI technical lead "Jason
  Chapman"; the calendar invite lists `jason.jasmin@partnersfcu.org` for the
  same role and background description (main point of contact, ran the
  original POC). Treated as the same person — the calendar email is the
  harder data point — but this was not independently re-confirmed.
- **The AE's own Dagster credit-calculator spreadsheet** (filled the same
  day as the discovery notes) defaults its "current orchestrator" field to
  "Airflow + dbt core" with placeholder model-count values. This conflicts
  with the discovery notes' explicit "SQL Server shop... no orchestrator,
  building workarounds," and with "Mark has used Airflow, Jason has not"
  (past personal experience, not current production tooling). Treated as an
  unfilled template default, not a real signal that Airflow is in
  production at Partners FCU — flagging so nobody in the room is surprised
  if it comes up.
- **Public DBA posting suggests some Azure footprint** (Azure SQL Managed
  Instance) alongside self-hosted SQL Server, which the AE notes don't
  mention at all. Not a contradiction, but worth confirming live rather than
  assuming pure on-prem.
- **No public corroboration exists (or could exist)** for the stalled
  year-old OSS pilot — entirely AE-notes-only, as expected for an internal
  engagement.
- **No BI tool, ingestion tool, or specific data domain is named anywhere** —
  three genuine "unknown"s, marked as such rather than guessed.

---

# Build directives

## Asset graph

This is a "prove platform completeness on three named asks" story, not a
large migration or multi-brand estate — keep it focused, ~11 assets:

- **`ingestion`** (source badged to match their real SQL Server shop):
  `raw_member_transactions`, `raw_deposit_accounts`, `raw_loan_originations`
  — daily time-partitioned. These represent exactly the sources whose
  ordering/dependency workarounds cause the named pain today.
- **`dbt`** (a real, small dbt Core project — staging then marts, native
  `dagster-dbt`, running for real): `stg_member_transactions`,
  `stg_deposit_accounts`, `stg_loan_originations` →
  `fct_daily_member_activity`, `fct_loan_portfolio_summary`, `dim_member`.
  dbt's own tests (not-null/unique/relationships) auto-surface as Dagster
  asset checks — the direct, no-extra-plumbing answer to "quality checks."
- **`reporting`**: one downstream asset, e.g.
  `member_activity_reporting_extract`. No BI tool is named anywhere in the
  AE notes or public research, so per house rules this asset gets **no
  kind badge** rather than a guessed one (no Power BI/Tableau/Looker
  invention).

**Partitions:** daily time partitioning on the transactional/fact assets.
Their own explicit ask is "talk through partitioned assets" — this is close
to the whole point of the demo, not incidental.

## Fidelity

**Stubbed (graph-first)**, per house default. Real dbt SQL runs regardless —
and that's exactly where the "quality checks" ask lives, so graph-first
already delivers what they asked to see. No hand-written Python asset needs
data-backed values; nothing in the brief demands specific numbers on screen.

## Demo shape

**Build-a-pipeline.** Not orchestrate-existing-workloads: the pain described
is ad hoc, hand-sequenced SQL Server jobs — "workarounds," not a named
external scheduler product with its own API to wrap and observe. Not
migration-both-states either: there's no named legacy system being
decommissioned in parallel; this is "replace ad hoc scripts with Dagster,"
which is squarely a build-a-pipeline / modernization story.

## Native integrations to use

- **`dagster-dbt`** — dbt Core is a real, currently-running tool, not a
  maybe.
- SQL Server as the source system for ingestion — search the registry (see
  below) for a real ingestion/IO-manager component before defaulting to a
  plain `AssetSpec`.
- Snowflake — **not confirmed, only "likely."** Badge the warehouse/dbt
  output layer `kinds={"snowflake"}` per the AE's own forward-looking
  framing, but do not build against real Snowflake credentials or treat it
  as a decided target — flag this kind choice as an assumption in the
  README.

## Community components to search for

- `sql server`, `mssql`, `azure sql` — ingestion/IO-manager candidates for
  the raw source layer (their actual, currently-running database).
- `snowflake` — IO manager, in case a real component is needed for
  demo-mode subclassing on the warehouse layer.
- `dbt` — should be covered natively; search anyway per the escalation
  ladder.

## Asset checks

At least three, mapped directly to named pain:

1. **Blocking** — completeness check (row-count floor) on
   `raw_member_transactions` / `raw_loan_originations` before the dbt layer
   computes on them. Direct reversal of "workarounds to get jobs to run in
   the right order... dependencies running properly": Dagster refuses to
   compute downstream on incomplete upstream data instead of needing a
   hand-built ordering workaround.
2. **dbt-native tests** (not-null/unique) on the `stg_*` models,
   auto-surfaced as Dagster asset checks — the direct, no-extra-plumbing
   answer to "quality checks."
3. **Warning** — reconciliation check on `fct_daily_member_activity` row
   counts vs. its raw source. Same pain, softer signal.

Since alerting is their single most-named ask, make sure these checks are
the ones wired to a Dagster+ alerting policy in the live walkthrough (see
Money shot) — the point is showing a real check failure would actually page
someone, not just exist.

## Demo mode

- **What must be mocked:** the SQL Server source connection (no real Partners
  FCU credentials exist), Snowflake (speculative future target, not real
  today), and the downstream reporting sink.
- **What can be real:** dbt Core, running for real, with real tests, against
  DuckDB standing in for the warehouse.
- **Asset kinds to display:** `mssql`/`microsoft_sql_server` on ingestion
  (their real, current source), `snowflake` on the dbt/warehouse output
  layer (per their own "likely adding" framing — flagged as an assumption),
  dbt's kind via translator override so it reads `{"dbt", "snowflake"}`
  rather than defaulting to `duckdb`. No kind on the reporting asset — no BI
  tool is confirmed.
- **Everything materializes green.** No planted failures — the alerting and
  restart-from-failure story is a live Dagster+ walkthrough and talk track,
  never a staged break.
- **Data realism notes:** Deterministic stub data only if row-count check
  thresholds need something to compare against; no synthetic data generation
  beyond that.

## Three buckets

- **In code:** the asset graph above, the three checks, daily partitions, a
  freshness policy on `fct_daily_member_activity` (the asset a BI team would
  page someone about), and an eager automation condition on the dbt layer so
  new raw data triggers a rebuild.
- **Dagster+ (demonstrate, never build):** **Alerting to Slack/email/
  PagerDuty** — the headline ask, wire a real check to a live alerting
  policy on screen, never hand-roll. Restart / rerun-from-point-of-failure —
  the direct fix for "any workaround needed to get dependencies right."
  Branch deployments — dev/test isolation, ties directly to the NCUA/GLBA
  vendor-risk framing (changes tested safely before touching production
  member data). Run history/logs, RBAC and viewer licenses, asset health and
  lineage UI — the substance of "vs. OSS," what self-hosting doesn't give
  them operationally.
- **Conversation only:** the Snowflake migration itself (kind badge reflects
  intent, no real build); any ticketing/webhook integration (none named);
  the broader BI-replatform roadmap beyond this pipeline.

## Demo name

**"Dependencies, Not Duct Tape"** — the whole pitch in four words: their own
named pain is hand-built job-ordering workarounds; the asset graph is what
replaces the duct tape.

## Money shot

Screen: the full lineage graph, green, SQL-Server-badged raw sources flowing
through the real dbt project (with dbt-test-derived checks visible in the
panel) into daily-partitioned fact assets. Click the blocking completeness
check on a raw asset, show it passing today, and explain what happens if it
goes red: downstream is refused, not silently computed on bad data — the
direct reversal of "workarounds to get jobs to run in the right order and
have dependencies running properly." Then open the Dagster+ alerting policy
screen and wire that same check to a Slack channel, live — the one thing
they explicitly said was untested in the OSS pilot and the reason this
meeting exists.

## Capability talk track

- **Partitioned assets (built):** daily partitions on the fact layer —
  directly answers their own explicit ask, "talk through partitioned
  assets."
- **Blocking asset checks (built):** downstream refuses to compute on bad
  upstream data — direct reversal of the named dependency-workaround pain.
- **dbt-native tests as checks (built):** quality checks with no extra
  plumbing to maintain — direct answer to "quality checks."
- **Alerting to Slack/email/PagerDuty (Dagster+, not built):** the single
  most-named ask ("they *really* care about alerting"); demonstrate policy
  configuration live, never hand-roll.
- **Restart / rerun-from-point-of-failure (Dagster+, not built):** the
  mechanical fix for needing manual job-ordering workarounds today.
- **Branch deployments (Dagster+, not built):** dev/test isolation before
  touching production member data — ties to the NCUA/GLBA vendor-risk
  framing.
- **RBAC, run history, asset health/lineage UI (Dagster+, not built):** the
  concrete "vs. OSS" answer across the board — what a year of self-hosting
  didn't give them.

## Explicitly out of scope

- No real Snowflake credentials or live Snowflake I/O — badge only, per
  "likely adding," not "decided."
- No BI tool build — none named; don't guess Power BI, Tableau, or Looker.
- No SSIS or legacy-scheduler external-asset modeling — the pain described
  is ad hoc workarounds, not a named external orchestrator product to wrap
  and observe.
- No planted failures or anomalies — the graph stays green; alerting,
  checks, and restart-from-failure are shown via Dagster+ walkthroughs and
  talk track, never a staged break.
- No invented member-data specifics beyond generic, recognizable
  credit-union nouns (deposits, loans, member transactions) — flagged as an
  assumption, not a confirmed table list.
