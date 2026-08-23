---
company: "Northwind Logistics"
slug: "northwind-logistics"
domain: "northwind-logistics.com"
demo_date: "2026-08-24"
demo_time: "10:00 AM ET"
attendees:
  - "jane@northwind-logistics.com — title unknown, only attendee exposed by the calendar invite"
  - "Priya Raghunathan — Director of Data Engineering — per AE discovery notes, expected in the room per AE's 'Next' note, not confirmed on the calendar invite itself"
  - "Marcus Boyd — Staff Data Engineer — per AE discovery notes, same caveat as above"
ae_doc: "https://docs.google.com/document/d/1USIym23jhojhs52OHbTnDBe4tsvP5qYKm8jUCU8ZRkU/edit"
ae_doc_modified: "2026-08-23"
overall_confidence: "medium"
generated: "2026-08-23"
---

# Northwind Logistics — demo brief

## Demo thesis

Priya's four-person data team (two of them hired this year) runs 340 undocumented
Airflow DAGs where dbt failures get swallowed by a BashOperator that exits 0, so the
first signal of a broken pipeline is a customer emailing about a missing invoice.
Carrier rate data lands late ~15% of the time and corrupts margin-by-lane reporting
until month-end close catches it — right as a new CFO is demanding trustworthy
margin-by-lane-and-customer numbers and a SOC2 renewal is coming up on lineage the
team punted on last cycle.

This demo has to prove three things. First, that asset-level lineage plus a check
that actually fails loud (not a green box hiding a red dbt run) catches the
late-carrier-data problem before it reaches the margin numbers. Second, that
**recovery is one partition with a computed blast radius**, not the manual backfill
work that ate Marcus's July. Third, that all of this is addable incrementally
alongside their existing Airflow estate, not a 340-DAG rewrite Marcus has to sign
up for on day one.

## Meeting

- **When:** 2026-08-24, 10:00–10:50 AM ET
- **Who's in the room:** Calendar invite only exposes `jane@northwind-logistics.com`
  (no display name or title surfaced by the calendar API). AE discovery notes name
  Priya Raghunathan (Dir. Data Eng, cares about audit-ready lineage and trusting the
  numbers she publishes) and Marcus Boyd (Staff DE, the technical skeptic who will
  push on migration cost) as the likely attendees based on the AE's "Next" note —
  **this is unconfirmed**, flag it as a gap.
- **Meeting type:** Demo (second touch — AE notes describe an earlier intro call
  with Priya alone, then a 30-minute discovery call with both Priya and Marcus)

## Use case — confidence: high

Northwind wants trustworthy, auditable margin-by-lane/customer reporting for a new
CFO, and lineage evidence for a Q1 SOC2 renewal where auditors already flagged the
gap once. Today neither is possible: pipeline failures are invisible until a
customer complaint, and margin numbers built on late carrier data go out wrong.

## Current stack — confidence: high

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Airflow 2.6, self-managed on EC2, ~340 DAGs | AE notes |
| Warehouse | Snowflake, ~8TB | AE notes |
| Transformation | dbt Core, ~600 models, invoked via BashOperator | AE notes |
| Ingestion | Fivetran (Salesforce, Zendesk, NetSuite) + homegrown Python for carrier APIs (FedEx, UPS, regional LTL) | AE notes |
| BI | Looker (not moving off it) | AE notes |
| Cloud | AWS | AE notes |
| CI/CD | Unknown — AE didn't ask | AE notes (gap) |

## Pain — confidence: high

Priya: *"we find out something broke when a customer emails us about a missing
invoice"* — no lineage, no alerting that means anything. The Airflow UI shows green
because the BashOperator wrapping dbt exits 0 even when the dbt run itself fails.

Marcus on the carrier API pulls: rate limits and silent partial failures force
manual backfills — he spent "most of July" on them.

Freight rate data lands late from two carriers ~15% of the time; downstream margin
reporting goes out wrong and nobody catches it until month-end close.

## Data domains

Freight/logistics: shipment events (~4M/day), carrier rate feeds (hourly during
business hours, daily overnight), nightly invoice/billing batch (must complete by
6am ET for finance). Peak season (Oct–Dec) runs ~3x normal volume — Priya is
nervous about this, and it's ~2 months away from the demo date.

## Public signals

- **Job postings:** No verifiable public footprint found for this specific entity.
  `northwind-logistics.com` returns no indexed pages, and every real "Northwind
  Logistics"/"Northwind Global Logistics" entity found via search (LinkedIn,
  Bloomberg, FMCSA/SAFER records) is a small single- or few-location freight
  carrier/broker — none match the ~2,200-employee mid-market 3PL described in the
  AE notes. Treat this prospect as effectively private/unlisted for research
  purposes; do not attribute any of the search hits above to this account.
- **Engineering blog / GitHub:** None found — see caveat above.
- **Recent news:** None found — see caveat above. AE notes mention a new CFO
  (started January) as the internal trigger; no independent confirmation.
- **Industry / compliance:** Industry-typical, not company-specific (confidence:
  low). SOC2 is close to table-stakes for mid-market logistics platforms selling
  to enterprise shippers/carriers, and auditors increasingly test data lineage
  (traceability from raw input to reported output) under Processing Integrity and
  Security criteria — consistent with, but not independent confirmation of, the
  AE's note that "auditors asked about data lineage last cycle."
- **Orchestration signals:** Industry-typical, not company-specific (confidence:
  low). dbt + Airflow + Snowflake is a very common mid-market combination, and
  "BashOperator wrapping dbt swallowing failures" is a well-known Airflow
  anti-pattern industry-wide — this corroborates the plausibility of the AE notes
  without confirming this company independently.

## Conflicts and gaps

- **No independent public corroboration at all.** This brief runs entirely on the
  AE discovery notes. Public search could not find a company matching the
  ~2,200-employee, mid-market 3PL profile in the notes under this name or domain.
  I'm flagging this explicitly rather than presenting industry-typical guesses as
  confirmed facts — walk in assuming everything outside "Current stack" and "Pain"
  above is AE-sourced only.
- **Attendee mismatch.** The calendar invite lists only `jane@northwind-logistics.com`
  with no name/title; the AE notes' "Next" line says the demo audience is Priya +
  Marcus. Unknown whether Jane is a new attendee (EA, additional stakeholder) or a
  scheduling proxy. No conflict between AE notes and public data otherwise — there
  simply is no public data to conflict with.
- **Snowflake may not be a long-term commitment.** Marcus made an unexplored
  comment about "what we're moving to." **Do not hard-assert Snowflake as their
  forever warehouse in the room.** If challenged, pivot: the asset graph, checks,
  and lineage are warehouse-agnostic — that's a feature, not a dodge.
- **Other unknowns the AE didn't get to:** CI/CD and dbt deployment process;
  budget number (only "platform line item" mentioned); full sign-off chain
  (assume CFO, unconfirmed).

---

# Build directives

## Asset graph

Group into four layers, ~16 assets total, named in Northwind's vocabulary.

**Ingestion**
- `carrier_rate_raw` — **multi-dimensional partitions: daily time dimension ×
  static carrier dimension** (`fedex`, `ups`, `regional_ltl_a`,
  `regional_ltl_b`). Both dimensions are required: the carrier dimension makes
  "one carrier is late" expressible, and the time dimension makes single-partition
  recovery surgical. If `MultiPartitionsDefinition` proves too costly to get right
  in the build window, fall back to daily time partitions with carrier as a
  column — but say so in the notification, because it weakens the recovery demo.
- `shipment_events_raw` — daily-partitioned, ~4M rows/day synthetic volume, scaled
  down deterministically for demo runtime
- `salesforce_accounts`, `zendesk_tickets`, `netsuite_gl_entries` — via
  dagster-fivetran, demo-moded

**Validation / staging**
- `carrier_rate_validated` — dbt model; carries the blocking arrival check. This
  is where the planted failure surfaces.
- `shipment_events_clean` — dbt model

**Transformation (dbt Core → dagster-dbt, replacing the BashOperator wrapping)**
- `shipments_by_lane`, `invoice_line_items`, `carrier_cost_allocation` — a subset
  of the ~600 real dbt models. Pick ~6–8 that feed the money-shot asset; do not
  attempt to port the whole project.

**Reporting**
- `invoice_billing_nightly` — daily-partitioned, SLA check: must be materializable
  before 6am ET
- `margin_by_lane_customer` — daily-partitioned. The money-shot asset: what the
  CFO wants and what Priya can't currently trust.

## Automation

- `AutomationCondition.eager()` on `margin_by_lane_customer` and
  `carrier_cost_allocation`, so they recompute on their own when upstream changes.
  **This is load-bearing for the recovery sequence** — without it, step 5 of Money
  Shot 2 requires manual clicking and the whole point collapses.
- A schedule on `invoice_billing_nightly` reflecting the 6am ET finance deadline.

## Native integrations to use

- `dagster-dbt` for the ~6–8 dbt models. This directly replaces the BashOperator
  anti-pattern that is the core pain point — **make that swap visible on screen.**
- `dagster-snowflake` for warehouse I/O, demo-moded to DuckDB. Use the
  **resource-swap variant** at the bottom of `templates/demo_mode_pattern.py`, not
  a `build_defs` override — the component stays completely unmodified, which is
  the strongest version of the pattern and the one that survives "does this work
  against our Snowflake?"
- `dagster-fivetran` for the three SaaS sources
- `dagster-airlift` as a **talking point, not a build item** — it answers Marcus's
  "what about our 340 DAGs" objection by observing/migrating incrementally rather
  than rewriting. Have the answer ready; do not spend build time on it.

## Community components to search for

- "REST API source" / "HTTP polling" / "rate limit" for the carrier rate feeds
  (FedEx/UPS/LTL) — no native Dagster integration covers arbitrary carrier rate APIs
- "Airflow" / "Airflow migration" — only to confirm current `dagster-airlift`
  coverage for the talking point. Do not build on it.

## Asset checks

1. **`carrier_rate_arrival` on `carrier_rate_validated` — BLOCKING severity.**
   Fails when a carrier partition hasn't landed within its expected window.
   It must be **blocking** so `margin_by_lane_customer` refuses to compute on bad
   input rather than computing a wrong number. A warning-only check reproduces
   their current situation with nicer UI and proves nothing. Maps to: "freight
   rate data lands late ~15% of the time."
2. **dbt run success surfaced as a real failure**, not a green Airflow box — a
   check on the dbt-model assets that fails loudly when the underlying dbt
   test/model fails. Maps to: "the Airflow UI shows green because the
   BashOperator exited 0 even when dbt failed."
3. **`invoice_batch_completeness` on `invoice_billing_nightly`** — verifies row
   counts/completeness against the 6am ET finance deadline. Maps to: "we find out
   something broke when a customer emails about a missing invoice."

## Demo mode

- **What must be mocked:** FedEx/UPS/regional LTL carrier rate APIs, Fivetran
  connectors (Salesforce/Zendesk/NetSuite), Snowflake (swap to DuckDB per
  `templates/demo_mode_pattern.py`).
- **What can be real:** DuckDB warehouse, **dbt Core executing for real against
  DuckDB**, deterministic seeded synthetic generators. Real dbt execution is
  strongly preferred over mocked — dbt is the centerpiece of the thesis, and
  simulated dbt lineage undercuts the whole argument.
- **Planted anomaly:** pin it concretely. `regional_ltl_b` for the `2026-08-21`
  daily partition arrives missing. Reference that exact partition key in the
  run-of-show. `carrier_rate_validated` fails its blocking check for that
  partition; `margin_by_lane_customer` for `2026-08-21` is visibly blocked rather
  than silently publishing wrong numbers. This is literally the scenario Priya
  described.
- **Data realism notes:** Keep the carrier mix at 4 (FedEx, UPS, 2 regional LTL)
  matching AE notes. Skew shipment volume ~3x on a couple of partitions to gesture
  at the Oct–Dec peak-season concern without building a seasonal model. Lane and
  customer cardinalities plausible for a mid-market 3PL.

## Money shot 1: the failure is caught — aimed at Priya

1. Asset graph. The `2026-08-21` / `regional_ltl_b` partition is red; downstream
   `margin_by_lane_customer` for that date is blocked, not green. Say the line:
   *their Airflow shows green right here.*
2. Click the check. It reads as a business fact, not a stack trace — something
   like *"Regional LTL carrier B rate data has not arrived for 2026-08-21; margin
   for this partition is blocked, not silently wrong."*
3. Trace lineage from the failed partition through to `margin_by_lane_customer`.
   That trace is the SOC2 evidence Priya deferred on last cycle.

## Money shot 2: targeted recovery — aimed at Marcus

Catching the failure answers Priya. This answers Marcus, who lost most of July to
manual backfills. **Do not cut this section for scope** — without it the demo only
addresses half the room.

4. Rematerialize **that single partition only** — one carrier, one day. Not the
   asset, not the month, not the DAG.
5. `carrier_cost_allocation` and `margin_by_lane_customer` recompute automatically
   for the affected partition alone, via the automation condition. Nobody clicks
   them.
6. Graph goes green. Under a minute, start to finish.

**Contrast line for Marcus:** in Airflow this is clearing a DAG run, guessing the
downstream blast radius, and re-running by hand — which is where his July went.
Here the partition is the unit of recovery and the blast radius is computed, not
remembered.

## Required demo-mode capability — do not skip

The anomaly is seeded deterministically so repeat runs are stable. A naive
rematerialize therefore regenerates **the same missing data**, and step 5 above
fails in front of the prospect. The demo component must support healing a
partition:

- Add `demo_healed_partitions: list[str] = []` alongside `demo_anomaly_partition`,
  **or** have the generator consult a small local state file (e.g.
  `demo_data/.healed`) that a materialization writes.
- **Heal wins.** A partition in the healed set generates clean data even when it
  matches `demo_anomaly_partition`.
- **The state file is strongly preferred**, because recovery then happens entirely
  inside the Dagster UI with no YAML edit and no terminal. Both hands need to be
  on the story, not on a text editor.
- **Must be resettable** so the demo can run more than once — a `make reset-demo`
  target or a reset job in the project. Document it in the README.
- Healing logic lives **only** in the demo subclass. The `demo_mode: false` path is
  untouched.

**Validation gate:** the build is not done until this loop has been executed end to
end — materialize all, confirm the blocking check fails on `2026-08-21` /
`regional_ltl_b`, heal, rematerialize that partition alone, confirm downstream
recomputes automatically and the graph goes green. If that loop doesn't work, say
so in the notification rather than reporting success.

## Explicitly out of scope

- Full migration of all 340 Airflow DAGs or all 600 dbt models — build ~6–8 dbt
  models and ~16 total assets, not a 1:1 port.
- `dagster-airlift` implementation. It's a talking point for Marcus; do not build
  on it.
- Looker — the BI layer stays theirs, not touched in the demo.
- Real SOC2 control mapping — the demo shows lineage/audit evidence *exists*, it
  doesn't attempt a compliance mapping exercise.
- Real carrier API integrations or real Fivetran connectors — everything upstream
  of the warehouse is demo-moded per the non-negotiables.
- CI/CD story — unknown from AE notes, don't invent one.
- Peak-season autoscaling — gesture at volume, don't model seasonality.
