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
team punted on last cycle. This demo has to prove that asset-level lineage plus a
check that actually fails loud (not a green box hiding a red dbt run) catches the
late-carrier-data problem before it reaches the margin numbers — and that this is
addable incrementally alongside their existing Airflow estate, not a 340-DAG
rewrite Marcus has to sign up for on day one.

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
- **Unknowns the AE didn't get to:** CI/CD and dbt deployment process; whether
  Snowflake is a long-term commitment (Marcus made an unexplored comment about
  "what we're moving to"); budget number (only "platform line item" mentioned);
  full sign-off chain (assume CFO, unconfirmed).

---

# Build directives

## Asset graph

Group into four layers, ~16 assets total, named in Northwind's vocabulary:

**Ingestion**
- `carrier_rate_raw` (partitioned by carrier: `fedex`, `ups`, `regional_ltl_a`,
  `regional_ltl_b`; hourly during business hours / daily overnight cadence)
- `shipment_events_raw` (daily-partitioned, ~4M rows/day synthetic volume, scaled
  down deterministically for demo runtime)
- `salesforce_accounts`, `zendesk_tickets`, `netsuite_gl_entries` (via
  dagster-fivetran, demo-moded)

**Validation / staging**
- `carrier_rate_validated` (dbt model; asset check for on-time arrival + rate
  discrepancy — this is where the planted failure surfaces)
- `shipment_events_clean` (dbt model)

**Transformation (dbt Core → dagster-dbt, replacing the BashOperator wrapping)**
- `shipments_by_lane`, `invoice_line_items`, `carrier_cost_allocation` (subset of
  the ~600 real dbt models — pick ~6–8 that feed the money-shot asset, don't try
  to port the whole project)

**Reporting**
- `invoice_billing_nightly` (partitioned daily, SLA check: must be materializable
  before 6am ET)
- `member_eligibility_daily` — **not applicable here**; replace with
  `margin_by_lane_customer` (daily-partitioned) — the money-shot asset the CFO
  wants and Priya can't currently trust

## Native integrations to use

- `dagster-dbt` for the ~6–8 dbt models (this directly replaces the BashOperator
  anti-pattern that's the core pain point — make that swap visible)
- `dagster-snowflake` for warehouse I/O (demo-moded to DuckDB per demo_mode_pattern)
- `dagster-fivetran` for the three SaaS sources
- Consider `dagster-airlift` as a talking point (not necessarily built) for
  incrementally observing/migrating existing Airflow DAGs — this directly answers
  Marcus's "what about our 340 DAGs" objection without requiring a rewrite

## Community components to search for

- "REST API source" or "HTTP polling" for the carrier rate feeds (FedEx/UPS/LTL) —
  no native Dagster integration covers arbitrary carrier rate APIs
- "Airflow" / "Airflow migration" (for dagster-airlift, confirm current component
  coverage before committing to it in the demo)

## Asset checks

1. **Carrier rate freshness/completeness** on `carrier_rate_validated` — fails when
   a carrier partition hasn't landed within its expected window. Maps to: "freight
   rate data lands late ~15% of the time."
2. **dbt run success surfaced as a real failure**, not a green Airflow box — a
   check on the dbt-model assets that fails loudly (not silently) when the
   underlying dbt test/model fails. Maps to: "Airflow UI shows green because the
   BashOperator exited 0 even when dbt failed."
3. **Invoice billing completeness/SLA check** on `invoice_billing_nightly` —
   verifies row counts/completeness before the 6am ET finance deadline. Maps to:
   "we find out something broke when a customer emails about a missing invoice."

## Demo mode

- **What must be mocked:** FedEx/UPS/regional LTL carrier rate APIs, Fivetran
  connectors (Salesforce/Zendesk/NetSuite), Snowflake (swap to DuckDB per
  `templates/demo_mode_pattern.py`).
- **What can be real:** DuckDB warehouse, dbt Core execution against DuckDB,
  deterministic synthetic shipment/invoice/carrier-rate generators (seeded).
- **Planted anomaly:** One regional LTL carrier's rate partition for a specific
  demo-day date arrives late/missing → `carrier_rate_validated` check fails for
  that partition → `margin_by_lane_customer` for the affected lane is visibly
  blocked/flagged rather than silently publishing wrong numbers. This is
  literally the scenario Priya described.
- **Data realism notes:** Keep carrier mix at 4 carriers (FedEx, UPS, 2 regional
  LTL) matching AE notes. Skew shipment volume up ~3x on a couple of demo
  partitions to gesture at the Oct–Dec peak-season concern without needing a
  full seasonal model.

## Money shot

Materialize `margin_by_lane_customer` for the affected date partition and show
the run stop cold on a red, human-readable asset check ("Regional LTL carrier B
rate data is 6 hours late for lane X — margin numbers for this partition are
blocked, not silently wrong") instead of a green Airflow DAG. Then show the
backfill: once `carrier_rate_raw` lands, re-materialize and watch the check go
green and the margin number update — this is "how we'd know something broke,"
delivered as asset lineage instead of a customer complaint.

## Explicitly out of scope

- Full migration of all 340 Airflow DAGs or all 600 dbt models — build ~6–8 dbt
  models and ~16 total assets, not a 1:1 port.
- Looker — BI layer stays theirs, not touched in the demo.
- Any real SOC2 control mapping — the demo shows lineage/audit evidence
  *exists*, it doesn't attempt an actual compliance mapping exercise.
- Real carrier API integrations or real Fivetran connectors — everything upstream
  of the warehouse is demo-moded per the non-negotiables.
- CI/CD story — unknown from AE notes, don't invent one.
