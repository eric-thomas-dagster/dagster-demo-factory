---
company: "Publicis Media (Publicis Groupe)"
slug: "publicis-media"
domain: "publicismedia.com"
demo_date: "2026-10-06"
demo_time: "11:00-11:30 America/New_York (30 min)"
attendees:
  - "Peter Balciunas — title not exposed by calendar (named in Dagster's 2025 Publicis engagement deck as someone we'd met) — peter.balciunas@publicismedia.com (accepted)"
  - "Mayank Solanki — title unknown — mayank.solanki@publicismedia.com (accepted)"
  - "Marzban Palsetia — VP Engineering, leads their data platform per Dagster's own 2025 exec-prep doc — marzban.palsetia@publicismedia.com (no response yet)"
  - "Christopher Pease — title unknown — christopher.pease@publicismedia.com (no response yet)"
  - "Dagster side: Laura Luuho (organizer), Eric Thomas; eric@prefect.io also invited"
ae_doc: "none for this meeting. Historical account notes exist and were used: https://docs.google.com/document/d/1bobdR7MZeaCwhH7zDXPkWwNQfT0v8nNULZshtlFdEN8/edit ('Publicis notes 12/3', last modified 2025-12-18) — stale, see Conflicts and gaps"
ae_doc_modified: "2025-12-18"
overall_confidence: "low"
generated: "2026-09-29"
---

# Publicis Media — demo brief

## Demo thesis

Publicis is already a Dagster+ Pro customer (signed order form, multiple
deployments included), and this 30-minute session is titled "Multi-tenancy" —
so nobody in the room needs convincing that Dagster orchestrates. What this demo
has to prove is that **one Dagster+ control plane can carry many agency and
client tenants at once — separate code, secrets, compute and permissions per
tenant — while lineage, checks and freshness still read as one platform.** Their
own words from the account history: with OSS "every client was on one [instance]
— now they want every client," and a client "doesn't want anything to be in
Amazon." The build has to show that adding the next tenant is one more block of
YAML in a shared component, not a new stack, and that a tenant-isolation check
guards the boundary.

## Meeting

- **When:** Tue 2026-10-06, 11:00–11:30 ET (Zoom, host Laura Luuho). One-off, not recurring, no POC/pilot wording in the title.
- **Who's in the room:** four `publicismedia.com` attendees (see frontmatter). Only Marzban Palsetia's role is documented (VP Engineering, "Pim's right-hand man", leads their data platform). What each cares about is **unknown** for this meeting; from the account history the platform-side concerns are standardization, self-service for agency pods, observability/trust, and per-region/per-client data separation.
- **Meeting type:** technical deep-dive on multi-tenancy with an **existing customer** — not a first demo. This is the "existing customer, one-off working session" case; the calendar-filter rules do not exclude it (not POC, not recurring, title is a technical topic), but flag it: if Eric considers a customer session out of scope for the factory, skip the build.
- **No agenda content readable.** The Zoom agenda link on the invite is not accessible from here. No Drive doc was shared for this meeting.

## Use case — confidence: medium (topic from invite title; framing from 2025 account notes)

Multi-tenancy on Dagster+: many agencies / client-facing teams / regional markets
running on the shared platform with isolation of code, secrets, compute and
access. Account history supplies the shape:

- "With OSS every client was on one; now they want every client" — scaling to "a lot more clients," "more clients, more data," more teams including data science (2/27 notes).
- Data-lake/clean-room product owner (Max Adamsky) on regional storage: "open to Dagster+ agents here… THIS IS A BIG ONE" — Dagster+ for multi-region, Fivetran for US/Canada; "Client like Walmart doesn't want anything to be in Amazon" → agents in different clouds (5/19 notes).
- "Would want the agency teams using the 'glitter layer' to be able to view Dagster, maybe even develop against it" (5/19).
- Wants "a platform so good that I can give users transparency into the process" (Max, 5/19).
- Pim's ask: needs to "see what issues are happening and when," "operationally managing this at scale," best practices from the beginning, "developers and product teams need to be able to manage" (9/11 prep call).

What the multi-tenancy meeting itself will ask is **unknown** — treat everything
above as inferred framing, not a confirmed agenda.

## Current stack — confidence: medium (2025 AE notes + current job postings; not re-confirmed in 2026)

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Dagster+ Pro (contracted; started on OSS, "prior to 1.0"). Airflow still present on other teams ("Python & airflow heavy", "Airlift (other teams on airflow)"). | AE notes; signed CSA in Drive; job posting lists Airflow + Dagster together |
| Warehouse | Databricks (Redshift → Databricks migration, "phase 1 finished… still work to do"; "Redshift to databricks to redshift to databricks with airflow") | AE notes; job postings (Databricks, Redshift-era AWS) |
| Transformation | dbt Core ("leaning heavily in dbt"); Python transformations; Spark/PySpark on Databricks | AE notes; job postings (Airflow/DBT, Spark) |
| Ingestion | Fivetran (investing; US/Canada regional use case), plus a homegrown internal tool ("Quartz scheduler" being sunset in favor of Fivetran); Google Analytics named as biggest-use-case source | AE notes (5/15, 5/19); job posting (London role names Fivetran + Databricks + Airflow/dbt) |
| BI | unknown | — |
| Cloud | AWS (S3 landing, AWS Marketplace purchase); AWS ID on file | AE notes; signed CSA |
| CI/CD | unknown (branch deployments and code locations flagged as "handy") | AE notes |

Also named in the notes: Alteryx contract ending this year (5/19). Not a demo
integration.

## Pain — confidence: medium (2025 notes; some of it may be resolved by now)

Quotes are from the AE's own notes; the AE's paraphrases of what the customer
said, not verified verbatim.

- "Multiple architectures going on, lots of silos between the teams."
- "Need metadata, no control" (Max on the global team).
- "Right now its a blackbox and we cannot see anything that is going on" / "Zero observability today."
- "One of the biggest things is trust in the platform."
- "Get backfill requests, bottleneck, its awful."
- "2–3 months to add new data sources."
- "Don't want ppl to come in and break everything but have access."
- "They have been in migration for a year and they do not have a single pipeline they consider in production."
- "Airflow keeps crashing" — wide blast radius (9/11 notes; also in the Sept 2025 deck: "Eliminate Outages").

The multi-tenancy angle collapses several of these into one: shared platform,
strong blast-radius and access boundaries.

## Data domains

Advertising / marketing data in a privacy-first setting: campaign delivery and
performance, web analytics (Google Analytics), audience and identity data,
clean-room-safe exports, plus data-science "audience insights" workbench inputs.
Volumes, SLAs and cadence: **unknown**. Cadence assumed daily for the demo
(assumption — flagged in the graph section).

## Public signals

- **Job postings:** Publicis Media/Groupe postings surfaced by search list Airflow, Dagster, Python, SQL, Spark, Databricks and AWS: an Associate Director, Data Engineering role names both Airflow and Dagster (Wellfound listing); Senior Data Engineer (Chicago) lists Airflow, AWS, Databricks at petabyte scale; a Senior Data Engineer (London) names Fivetran, Databricks and Airflow/dbt. Search returned several Publicis Media/Groupe data-engineering listings across job boards; I did not verify or count the roles currently open. Dagster appearing in a JD is a real signal of production use. Not verified: whether the listings are currently open.
- **Engineering blog / GitHub:** nothing found in the searches run.
- **Recent news:** H1 2026 results — Q2 organic growth +4.8%, full-year guidance raised to +4.5–5%, record H1 margin; reported >$3B of 2026 acquisitions (160over90 and LiveRamp named), with a stated pause on further large deals to focus on integration. €300M CoreAI investment announced in 2024, built in-house by Publicis Sapient. Acquisition integration (e.g., LiveRamp) is a plausible driver of more tenants and data sources, but that is my inference, not something found in a source.
- **Industry / compliance:** GDPR, privacy-first clean room and data-collaboration positioning (Publicis Global Data Privacy Office). Per the AE notes: DPA and due diligence completed by their privacy team; SOC2/GDPR/HIPAA raised in the security review.
- **Orchestration signals:** Airflow + Dagster co-listed in JDs; AE notes show Airflow crashing, "Airlift" for other teams, and a Redshift-to-Databricks migration still in flight.

## Conflicts and gaps

- **Stale source.** The only AE notes are the "Publicis notes 12/3" doc, last modified 2025-12-18 (its entries are un-year-labeled; I read them as 2024-12 through 2025-12 from context). Nothing dated 2026 describes the multi-tenancy ask. **Gap: unknown what they will actually ask on 10/6.**
- **Airflow.** AE notes say the goal is not to rip out Airflow ("this is where Airlift can come into play"); the current JDs list Airflow *and* Dagster. Consistent with coexistence, but I cannot tell from public data which teams are on which today. Unknown.
- **Which Publicis entity.** The signed Cloud Service Agreement in Drive names "Customer's Affiliate Publicis Media" as authorized (Dagster+ Pro, 1 year, 100 launcher/editor/admin seats, multiple deployments included). The earlier AE notes discuss the broader Groupe/CoreAI/Sapient/Epsilon estate. Whether the four attendees are Media-agency platform staff or Groupe-level is **unknown**.
- **Warehouse.** AE notes say Databricks; the London JD still names Airflow/dbt and Fivetran alongside it, and Redshift shows up in the migration story. No conflict on Databricks-as-destination, but the Redshift legacy side may still be live for some teams.
- **Titles and roles** of Mayank Solanki, Christopher Pease and Peter Balciunas are **unknown** from the calendar.
- No public evidence found of how Publicis structures clean-room tenancy; I searched and found nothing specific, so the tenant list below is my assumption.

---

# Build directives

## Asset graph

**Size: ~20 assets.** Multi-tenancy is a *breadth* story (many tenants on one
platform), so it needs enough tenants to look real, but each tenant is the same
small shape — one YAML mapping table, not per-tenant code.

**Tenant dimension (assumption, confidence low):** three Publicis Media agency
tenants using their **public brand names** as labels — `spark_foundry`,
`starcom`, `zenith` — plus a fourth `shared_platform` tenant for the central data
team. No real client brands (Purina, Colgate, etc. appear in the notes but are
not to be used). Swap the labels in one YAML block if Eric prefers neutral names
(`agency_a`, `agency_b`, …).

**Per tenant (×3), 5 assets each = 15:**
1. `<tenant>_ga_sessions_raw` — ingestion, kinds `fivetran`, `s3` (Google Analytics named as the biggest source). Partitioned by day × market.
2. `<tenant>_campaign_delivery_raw` — ingestion, kinds `s3`, `databricks`. Partitioned by day × market.
3. `<tenant>_stg_campaign_performance` — dbt staging, kinds `dbt`, `databricks`.
4. `<tenant>_fct_media_measurement` — dbt mart, kinds `dbt`, `databricks`.
5. `<tenant>_clean_room_export` — the tenant-scoped, privacy-safe export (custom logic, Axis 2 stub); kinds `databricks`.

**Shared platform layer (2):**
- `shared_identity_reference` — kinds `databricks`; consumed by all three tenants' staging (the one cross-tenant edge, deliberately read-only reference data).
- `platform_tenant_cost_rollup` — a plain reporting rollup keyed by tenant (kinds `databricks`).

**Legacy side (coexistence, pattern 4) — 3 assets:**
- Redshift-era feeds still owned by another team's Airflow: `legacy_redshift_audience_feed` (external asset, kind `redshift`, `airflow`), observed only, never triggered, consumed by `shared_identity_reference`. Plus two Airflow-owned DAG assets on the legacy side, via the Airflow/Airlift integration surface. The AE notes say plainly "probably not want to rip out airflow… Airlift can come into play," so the boundary is visible in the graph with `legacy_system_boundary`.

Total: 15 + 2 + 3 = **20**. If anything must go, cut the two extra Airflow assets first, never the per-tenant shape.

**Partitions:** `MultiPartitionsDefinition` of daily (trailing 14 days) × market `{US, CA, UK}` — US/Canada from the Fivetran regional note, UK from the London posting. The market dimension is the residency story: "which agent serves which market." Cadence daily is an **assumption**.

**Scaling shape (the point of the demo):** one component, one `tenants:` mapping table in `defs.yaml`, N tenants. Adding the fourth agency = one more block in the mapping. Laid out under `defs/` as one folder per tenant only if that keeps each `defs.yaml` a one-liner pointing at the shared component; otherwise a single mapping file. Do not write per-tenant Python.

**Metadata on every asset** (per house rules), with tenant-specific values: `tenant` (also as a tag), `owner`, `owner_team` (the tenant's team), `tier`, `domain: advertising_measurement`, `deployment_mode: hybrid`, `data_residency` (per market), `control_plane_egress: metadata_only`, `integration_pattern`, `legacy_system_boundary`, `business_impact`, `sla`. Groups: one per tenant plus `shared_platform` and `legacy_airflow`.

## Fidelity

**Stubbed.** All hand-written asset bodies (the clean-room export and the cost rollup) are `pass`; this is a lineage / isolation / metadata story and needs no fabricated numbers. Integration surfaces (Fivetran, Airflow/Airlift, dbt) are real components regardless. Real dbt SQL runs against local DuckDB with seeded synthetic data so the dbt-backed checks have something real to query; row counts must be deterministic.

## Demo shape

**Orchestrate-existing-workloads with one migration-both-states edge**, expressed as **multi-tenant build-a-platform**: the demo's subject is the tenancy model, so the graph is per-tenant parallel lanes on one control plane, with a single legacy Airflow/Redshift lane feeding in (pattern 4, coexistence — Airflow stays master for the legacy lane).

## Native integrations to use

Only what is named in the notes or evidenced publicly:

- `dagster-dbt` — dbt Core is named in the notes and JDs.
- `dagster-databricks` — Databricks is the migration destination in both.
- `dagster-fivetran` — Fivetran is named in the notes and the London JD.
- `dagster-airlift` / `dagster-airflow` — Airlift is named in the AE notes for the teams that stay on Airflow.
- `dagster-aws` (S3) — landing zone per the notes.

Redshift and BI are legacy/unknown; do not assume a BI tool.

## Community components to search for

Search terms (build routine runs the searches and records them):
`fivetran`, `databricks workspace`, `airflow`, `airlift`, `redshift`, `s3 observation` / `s3 sensor`, `tenant` / `multi-tenant`, `partition`, `asset check`, `freshness`. If nothing fits multi-tenant enumeration under one instance, subclass the Databricks or Fivetran workspace component (rung 3) rather than inventing a "tenant" component — the name must identify a system, not a technique.

## Asset checks

Every check maps to a pain above. Built once as factories, instantiated per tenant.

1. **Blocking — `campaign_delivery_arrival`** on each `<tenant>_campaign_delivery_raw`: partition present for each market. Answers "trust in the platform"; downstream refuses to compute on missing delivery data.
2. **Blocking — `clean_room_export_tenant_isolation`** on each `<tenant>_clean_room_export`: every row/entity in the export carries only that tenant's id. Answers "don't want ppl to come in and break everything" and the client-separation ask; this is the check that makes the tenancy claim testable.
3. **Warning — `clean_room_export_no_raw_identifiers`** on the same export: schema contains hashed identifiers only, no raw PII columns. Answers the privacy-first / GDPR posture.
4. **Warning — `ga_sessions_schema_drift`** on `<tenant>_ga_sessions_raw`: expected columns present. Answers "2–3 months to add new data sources."

Plus the dbt-native tests (real `schema.yml`) on the staging and mart models.

**Freshness policies:** on each `<tenant>_fct_media_measurement` (the thing a client would page someone about).
**Automation conditions:** `AutomationCondition.eager()` on the dbt layer and the export, gated on the blocking checks; one daily schedule for ingestion at a plausible hour (assumption).
**Observation sensors:** turn on the polling sensors for Fivetran and Airflow components (`generate_sensor: true` or the component's equivalent) so externally triggered runs show up.

## Demo mode

- **What must be mocked:** Fivetran account/connectors, Databricks workspace/jobs, Airflow instance (Airlift), S3. No credentials for any of them. Subclass the real components and swap the I/O boundary per `templates/demo_mode_pattern.py`.
- **What can be real:** DuckDB for the dbt layer, local files for the mock source state under `demo_data/`.
- **Asset kinds to display:** `fivetran`, `s3`, `databricks`, `dbt`, and `redshift` + `airflow` on the legacy lane. The engine is DuckDB; the UI must not say so.
- **Everything materializes green.** No planted anomaly, corrupted partition or failure scenario.
- **Data realism notes:** 3 tenants × 3 markets × 14 days; small, deterministic row counts (seeded); UK/CA volumes smaller than US.

## Three buckets

- **IN CODE:** per-tenant asset lanes from one component/mapping table; the four checks; freshness policies; eager automation; tenant/market metadata and tags; kinds; the legacy Airflow/Redshift lane and its observation sensor; dbt project.
- **DAGSTER+ (show in the UI, never implement):** code locations as tenants (`dagster_cloud.yaml`), separate secrets/env vars per code location, RBAC and teams, agent routing to dedicated agents (noisy-neighbor isolation), separate deployments as a hard boundary, hybrid agents in different clouds/VPCs/regions, branch deployments, audit log, alerting to Slack/Teams/email/PagerDuty, catalog and lineage views, run history, Insights/cost per code location.
- **CONVERSATION (no code):** their residency architecture across clouds/regions (Fivetran regional footprint), Publicis Sapient's "rigid framework"/enforced-standards model, the agency-team self-service and onboarding process, Airflow-to-Dagster migration sequencing beyond Airlift, commercial/credit projection. Note where an event could feed an external system; build nothing.

## Demo name

**"One Platform, Every Agency"**

## Money shot

In the Dagster+ UI on a green graph: the asset graph is filtered by the `tenant` tag, so the Spark Foundry lane, the Starcom lane and the Zenith lane each stand alone with their own owners, kinds and market partitions — and all three edge into `shared_identity_reference` and out to the same cost rollup. Then open `starcom_clean_room_export` and show its `clean_room_export_tenant_isolation` check, configured, passing, with the metadata panel showing tenant, residency and the "hybrid / metadata-only egress" posture. Say: "One control plane, three tenants; the isolation boundary is a check you can read, and the fourth agency is one more block in this file." Open the mapping `defs.yaml` and show that block. Then switch to the Dagster+ Deployment/Code locations page and show how each tenant maps to its own code location and agent (platform capability, not built here).

## Capability talk track

- **Tenant isolation check (blocking):** if an export ever carries another tenant's id, downstream refuses to compute and the export never reaches the clean room. In production that fires on the tenant lane only, the other lanes stay green — blast radius equals one tenant, the direct answer to "Airflow keeps crashing" with a wide blast radius.
- **Arrival check (blocking):** if a market's delivery feed hasn't landed, the measurement mart won't compute on a partial day. Answers "trust in the platform."
- **No-raw-identifiers check (warning):** flags schema regression toward raw PII before it reaches a client; ties to the privacy-first posture.
- **Freshness policy on the measurement mart:** how the platform team finds out before the agency does. Alert routing to Slack/Teams/PagerDuty per tenant is Dagster+ native — point at it, don't build it. Custom alerting: not needed; no gap identified.
- **Eager automation:** what recomputes when a tenant's raw feed lands, and why it waits on the blocking checks.
- **Backfills:** the "backfill requests, bottleneck" pain — targeted partition backfill for one tenant/market from the Dagster+ backfill UI; targeted re-run is a platform capability shown live, never coded.
- **Observation sensors:** runs started outside Dagster (Airflow, Fivetran) still appear in lineage — the coexistence line: "Airflow stays master for what it owns; Dagster owns the new estate; one graph."
- **Mode statement (must appear in README and notification):** live demo runs locally with `dg dev`; Dagster+ deployment proves the project loads and the graph renders. Multi-code-location, agent routing, RBAC and deployment-level tenancy are shown in Eric's existing Dagster+ org, not produced by this repo.

## Explicitly out of scope

- One code location per tenant in the deployed demo. The factory deploys a single `demo-publicis-media` location; the folder-per-tenant layout is what makes splitting it later a one-line move. Show real multi-location tenancy in the platform UI.
- Any RBAC, agent-routing, secrets-scoping or audit code. All Dagster+.
- Custom alerting, cost-tracking or "tenant onboarding" jobs.
- Real Publicis client brand names, real data, or anything implying real Publicis systems beyond the public agency labels.
- Clean-room vendor integrations (LiveRamp, Epsilon, Snowflake/Databricks clean rooms); not named in a source as in scope for this meeting.
- A demo-control or reset asset; if the mock source needs resetting, it is a script outside Dagster.
