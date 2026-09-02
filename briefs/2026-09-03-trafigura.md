---
company: "Trafigura Group"
slug: "trafigura"
domain: "trafigura.com"
demo_date: "2026-09-03"
demo_time: "8:00–8:30 AM (calendar's raw UTC-4 offset; UI labels the zone America/Chicago, which doesn't match — same connector display quirk seen on the RVU invite). 30 minutes."
attendees: ["sam@prefect.io — organizer, role/identity on this specific call unclear", "eric@prefect.io — attendee, role unclear", "Eric Thomas — Dagster SE — eric.thomas@dagsterlabs.com", "No Trafigura-domain attendee appears on the invite at all — see Meeting type below"]
ae_doc: "none"
ae_doc_modified: ""
overall_confidence: "low"
generated: "2026-09-02"
---

# Trafigura Group — demo brief

## Demo thesis

**Read this section before the others — the confidence caveat matters more
than usual here.** There are no AE discovery notes for this meeting, and
unlike every other brief in this repo, the calendar invite itself lists
**zero Trafigura attendees** — only `sam@prefect.io`, `eric@prefect.io`, and
Eric Thomas. It's genuinely unclear whether this is a customer-facing call
with Trafigura people missing from the invite, or an **internal walkthrough
between colleagues** rehearsing what a future Trafigura pitch would look
like. Everything below is built from public research only, to be useful
either way — but confirm which one this actually is before walking in.

With that said, two independently verifiable public facts make a strong,
literally-true "why now" if this does turn into a real Trafigura
conversation: Trafigura created a brand-new CIO role in January 2026
(Jane Kilmartin, ex-Alpiq) explicitly covering Trading IT, Digital
Transformation, **Data Science & Engineering**, and Risk IT — a new
tech leader, less than a year in, with a mandate spanning exactly the
systems this demo would touch. Separately, Trafigura disclosed two major
internal fraud incidents in the last two years (nickel trading, ~2023;
Mongolia oil business, ~2024) in which the company's own reporting
described **rogue employees manipulating data and documents undetected for
years**, triggering a public governance-and-controls overhaul overseen by
its CRO and COO. That makes asset-level lineage, checks, and audit
trail a topic this company's leadership is already primed to care about at
a governance level, not just an engineering nice-to-have — though there is
**no evidence this specific meeting is about that**, and it should be
raised as context, not assumed as the meeting's actual subject.

## Meeting

- **When:** 2026-09-03, 8:00–8:30 AM per the calendar's literal offset — 30
  minutes, short for a full technical demo.
- **Who's in the room:** Unknown on the Trafigura side — no Trafigura-domain
  attendee is listed on the invite. Attendees are `sam@prefect.io`,
  `eric@prefect.io`, and Eric Thomas.
- **Meeting type:** **Ambiguous.** Titled "Trafigura Dagster+ Walkthrough,"
  which combined with the all-internal attendee list reads more like an
  internal enablement/rehearsal session than a live customer call. Treat
  this brief as prep material either way, but verify which one it is —
  the two situations call for very different levels of readiness.

## Use case — confidence: low

No discovery notes exist. Trafigura is one of the world's largest
independent commodity trading groups (oil and petroleum products, metals
and minerals, gas, power, renewables, carbon trading, plus shipping/marine
logistics and storage) — Singapore-headquartered with major hubs in Geneva,
Houston, Montevideo, and Mumbai, ~14,500 employees, ~$243B FY2024 revenue.
At this size, "the use case" could be almost anything from a single trading
desk's data pipeline to a firm-wide platform initiative — there is no
signal narrowing it down. Do not guess a specific initiative name.

## Current stack — confidence: low-medium

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Not named anywhere publicly | Gap, not confirmed either way |
| Warehouse | AWS Redshift (at least for the Gas & Power desk) | Job posting, direct |
| Transformation | Not named — no dbt or equivalent evidenced | — |
| Ingestion | AWS Glue, S3, Lambda-based ETL/ELT (Gas & Power desk) | Job posting, direct |
| BI | Power BI | Job posting (BI-liaison role), direct |
| Cloud | AWS, at least for Gas & Power's "cloud native big data platform" | Job posting, direct |
| CI/CD | Unknown | — |

**Important scope caveat:** all of the above is evidenced specifically for
the **Gas & Power trading desk** (a "Front Office Data & Analytics Engineer
– Gas & Power Trading" role in Geneva, a "Data Engineer" role for "North
American Gas and Power" data integration, both AWS/Python/SQL-based). A
company this size, trading multiple distinct commodity classes, plausibly
runs different stacks per desk (oil, metals & minerals likely differ) —
don't assume Gas & Power's stack applies firm-wide.

## Pain — confidence: low

No stated pain exists anywhere — no AE notes, no direct quotes. The
closest public signal is structural rather than a named complaint: Trafigura
has publicly committed to a "significant governance and controls project"
following the fraud incidents described above, and created a new CIO role
spanning Data Science & Engineering and Risk IT to drive it. That is
evidence of an organizational priority, not a specific technical pain point
— do not put words in anyone's mouth about what's broken today.

## Data domains

Not confirmed. Generic commodity-trading domains, inferred from the
business and from the one concretely evidenced desk (Gas & Power): trade
capture/deal records, counterparty and credit exposure, market/reference
data (commodity prices, FX, freight rates), and downstream risk/BI
reporting. Treat as illustrative scaffolding, not a confirmed data model —
no real table or system names exist in any source.

## Public signals

- **Job postings:** Several concurrent Trafigura data roles — "Front Office
  Data & Analytics Engineer – Gas & Power Trading" (Geneva), "Front Office
  D&A Application Engineer" (Houston), a "Data Engineer" for North American
  Gas and Power, a "Data Analytics Support Analyst" supporting "the data and
  analytics platform used by research and trading teams," a BI-liaison role
  managing a Power BI backlog, and a two-year Geneva-based Technical
  Graduate Programme rotating through "Data Science & Engineering, Trading
  IT, and Research Analysis" (Python, Java, React, "developing data products
  for a large data platform"). Consistent picture: active, ongoing data
  engineering hiring across multiple functions, not a one-off role.
- **Engineering blog / GitHub:** Not checked in depth given time constraints
  for this meeting; none surfaced incidentally.
- **Recent news:** New CIO Jane Kilmartin (ex-Alpiq Group CIO), appointed to
  a **newly created role**, announced January 2026, explicitly overseeing
  Trading IT, the Digital Transformation Team, Data Science & Engineering,
  Risk IT, IT Infrastructure, and IT Security — a strong "new platform
  leader" signal. Separately: a ~$500–577M nickel-trading fraud (disclosed
  2023) and a ~$1.1B Mongolia oil-business fraud (disclosed 2024), both
  described in Trafigura's own reporting as multi-year, undetected data and
  document manipulation by employees, which triggered a governance and
  controls overhaul overseen by the CRO/COO. Handle this in the room with
  care — it's real, serious, and public, but it's context for why data
  governance might matter to them, not a talking point to lead with or
  reference glibly.
- **Industry / compliance:** Commodity trading carries derivatives-reporting
  obligations (EMIR in the EU, Dodd-Frank in the US), sanctions/AML/KYC
  screening given the geographic breadth of counterparties, and — per the
  above — an active, board-level internal-controls mandate specific to this
  company right now.
- **Orchestration signals:** No orchestrator named anywhere publicly. AWS
  Glue provides some native workflow capability but isn't a dedicated
  orchestration layer — read this as a genuine gap, not a migration-away
  signal, same as the Detroit DWSD situation.

## Conflicts and gaps

- **No AE discovery notes exist at all** — the biggest gap in this brief by
  far. Everything above is public-research inference.
- **No Trafigura attendee is on the calendar invite.** This may not be a
  customer-facing meeting. Confirm before treating this as demo day.
- **Stack evidence is desk-specific (Gas & Power), not firm-wide** — do not
  build as if Trafigura's whole trading operation runs on Redshift/Glue;
  say "at least one desk" rather than asserting a company-wide stack.
- **The fraud/governance angle is a plausible "why now," not a confirmed
  one.** No source ties the CIO's mandate or any planned initiative
  specifically to a data-lineage or orchestration project. Raise it as
  context if it comes up naturally; don't force it into the pitch.

---

# Build directives

## Asset graph

Small-to-medium, generic commodity-trading graph (~12–14 assets) — sized
for a 30-minute walkthrough of uncertain audience, not a firm-wide platform
pitch. Deliberately generic rather than Gas-&-Power-specific, since desk
scope is unconfirmed:

- **`market_data`**: `commodity_price_feed_raw`, `fx_rate_feed_raw`,
  `freight_rate_feed_raw`.
- **`trade_capture`**: `trade_capture_raw`, `counterparty_reference_data_raw`.
- **`risk_warehouse`** (badged `redshift`, the one confirmed warehouse
  product): `dim_counterparty`, `dim_commodity`, `fact_trade_position_daily`,
  `fact_credit_exposure_daily`.
- **`reporting`**: `power_bi_trading_risk_dashboard`.

**Partitions:** daily on `fact_trade_position_daily` and
`fact_credit_exposure_daily` — end-of-day position/risk is a standard
trading-desk cadence and needs no further evidence to justify. Don't add a
second (e.g., per-desk) partition dimension — nothing confirms firm-wide
scope.

## Fidelity

**Graph-first.** No notes exist asking for computed values on screen, and
with total uncertainty about the audience, minimizing build risk and time
is the right call by default. Asset bodies are `pass`.

## Demo shape

**Build-a-pipeline.** No legacy orchestrator or existing workload is
evidenced to trigger-and-observe against — this is a from-scratch
illustrative pipeline, not a migration or coexistence story.

## Native integrations to use

- **AWS (Redshift, S3, Glue)** — the one concretely evidenced stack,
  scoped to Gas & Power specifically; badge `kinds={"redshift"}` /
  `kinds={"s3"}` on the relevant layers.
- **Power BI** — evidenced via the BI-liaison job posting; search the
  registry for a Power BI component before building a plain AssetSpec.
- **No dbt, no Snowflake, no Databricks** — none evidenced for this
  specific scope; don't add them on habit.

## Community components to search for

- `power bi` (rung 2/3 candidate)
- `redshift` / `aws glue` (native integrations should cover the warehouse
  and ingestion layers; search anyway per the escalation ladder)

## Asset checks

At least three, generic but legible against a commodity-trading domain:

1. **Blocking** — `trade_capture_raw` completeness: every trade record has
   a valid `counterparty_id` and `commodity_id` before flowing downstream.
2. **Blocking** — `fact_credit_exposure_daily` reconciliation: exposure
   figures reconcile against source trade capture within tolerance.
3. **Warning** — `commodity_price_feed_raw` freshness/staleness: market
   data arrives within its expected window.

Frame these in the room as standard data-quality practice for a trading
platform — do not reference the fraud incidents as the reason these checks
exist; let the checks speak for themselves.

## Demo mode

- **What must be mocked:** All sources — no real Trafigura systems or
  credentials exist. Redshift/S3/Glue/Power BI are demo-mode mocked per
  `templates/demo_mode_pattern.py`.
- **What can be real:** DuckDB standing in for Redshift locally.
- **Asset kinds to display:** `redshift`, `s3`, `powerbi`. Nothing more
  specific — no ETRM or trading-system product name is confirmed.
- **Everything materializes green.** No planted failures.
- **Data realism notes:** Not applicable — graph-first, no synthetic data.

## Three buckets

- **In code:** The asset graph above, three checks, freshness policy on
  `fact_credit_exposure_daily`, eager automation on the warehouse layer.
- **Dagster+:** Alerting, RBAC (relevant to a company this size with
  distinct trading desks), restart-from-failure, lineage visualization, run
  history, asset health — demonstrate, don't build.
- **Conversation only:** Any firm-wide platform strategy, the specific
  governance/controls initiative referenced above, and anything
  desk-specific to oil or metals & minerals — no evidence supports building
  for those.

## Demo name

**"One Ledger, Every Desk"** — deliberately generic and provisional, since
actual scope is unconfirmed; rename freely once real context exists.

## Money shot

Screen: the full lineage graph, green, market data and trade capture
flowing into a risk warehouse and out to a Power BI dashboard. Click
`fact_credit_exposure_daily`: show its reconciliation check passing, its
freshness policy, and its lineage back through `trade_capture_raw` to the
original source. The pitch is simply "every number in this dashboard has a
visible, checked path back to where it came from" — let the audience draw
their own connection to governance if they're going to; don't draw it for
them.

## Capability talk track

- **Asset checks (built):** standard data-quality practice — trade
  completeness and exposure reconciliation, framed neutrally.
- **Freshness policies (built):** how a desk would know market data went
  stale before it affected a position.
- **Lineage graph (built, native):** full path from source feed to
  reported number.
- **RBAC, alerting, restart-from-failure (Dagster+, not built):** platform
  capabilities relevant to a multi-desk organization at this scale — show
  live if the call goes into a full demo.

## Explicitly out of scope

- No firm-wide Trafigura platform build — desk-scoped, illustrative graph
  only.
- No specific reference to the nickel or Mongolia fraud incidents inside
  the demo itself — real, public, sensitive; background context only, not
  a script line.
- No ETRM/trading-system product names — none confirmed.
- No planted failures.
- No assumption that this is a live customer call — verify that first.
