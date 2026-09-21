---
company: "MarketGrader.com"
slug: "marketgrader"
domain: "marketgrader.com"
demo_date: "2026-09-25"
demo_time: "11:30 AM–12:30 PM America/New_York. 60 minutes."
attendees: ["Carlos Diez (diez@marketgrader.com) — Founder & CEO, the invite is addressed to him personally (\"Hi Carlos\")", "pdelgado@marketgrader.com — role unknown", "smuller@marketgrader.com — role unknown", "jandrews@marketgrader.com — role unknown", "jbermudez@marketgrader.com — role unknown", "Naomi Mastico — Dagster SE, organizer", "Eric Thomas — Dagster SE"]
ae_doc: "none"
ae_doc_modified: ""
overall_confidence: "low"
generated: "2026-09-21"
---

# MarketGrader.com — demo brief

## Demo thesis

**No AE discovery notes exist for this meeting at all** — checked Drive
(company name, domain, every attendee surname, "MEDDIC", "discovery",
"technical qualification") and Gmail; the only artifact in either system is
the calendar invite itself, created same-day by Naomi and addressed to
"Carlos" (almost certainly Carlos Diez, MarketGrader's founder/CEO, who is
on the invite). This brief is built entirely from public research on a
company with very little public footprint — no job postings, no
engineering blog, no GitHub org, Crunchbase/ZoomInfo/Form ADV all thin. It
is genuinely a best-guess brief; lean generic, not specific, everywhere the
brief says so.

What public data does establish: MarketGrader is a small (~11-15 employee),
bootstrapped, 25+ year old quant-research shop that scores 39,000-41,000+
public companies daily across 24 fundamental indicators (growth, value,
profitability, cash flow) to produce buy/hold/sell ratings, and its sister
entity MarketGrader Capital licenses the resulting indexes — including the
**Barron's 400 Index**, tracked by a real ETF — to fund sponsors,
institutions, and separately managed accounts. That is the shape the demo
should target even without discovery notes: a **daily, large-fan-out
financial-data scoring pipeline whose output becomes other people's money**.
For a team this size running something with that much downstream financial
consequence, the pitch that should land is "lineage and blocking checks
between raw fundamentals ingestion and published index constituents" — not
a specific tool swap, since no tool is confirmed. Confirm the real use case
live in the room before leaning on any of the build specifics below.

## Meeting

- **When:** 2026-09-25, 11:30 AM–12:30 PM America/New_York. 60 minutes —
  titled "Dagster Labs Demo/Deeper Dive with Naomi," booked via Chili Piper
  the same day this brief was generated (created 2026-09-21).
- **Who's in the room:** Carlos Diez (founder/CEO, per the invite's personal
  greeting) plus four other marketgrader.com attendees whose roles are not
  discoverable from any connected source — pdelgado, smuller, jandrews,
  jbermudez. Titles unknown; do not guess them in the room.
- **Meeting type:** First demo / deeper dive. Not a POC check-in, not
  recurring — this is a fresh opportunity.

## Use case — confidence: low

No discovery notes, so this is entirely inferred from what MarketGrader
publicly does. They run one core daily process at real scale: pull
fundamental and market data for 39,000-41,000+ global public companies,
compute 24 indicators per company, aggregate into a 0-100 score, and
publish buy/hold/sell ratings and index memberships from it. A second
process — index construction/licensing — consumes those ratings to
determine and publish constituents for MarketGrader's family of indexes
(including the Barron's 400), which are then licensed out to third parties
who build real financial products (ETFs, SMAs, model portfolios) on top of
them. Nothing else about their orchestration, data engineering team, or
current pain is known. Treat "why they're talking to Dagster" as entirely
open going into the room.

## Current stack — confidence: low

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Unknown | No signal found anywhere |
| Warehouse | Unknown | No signal found anywhere |
| Transformation | Unknown | No signal found anywhere |
| Ingestion | Unknown — likely one or more commercial fundamentals/market-data vendors, none named | Inferred from business model only |
| BI | Unknown | No signal found anywhere |
| Cloud | Unknown | No signal found anywhere |
| CI/CD | Unknown | No signal found anywhere |

This table is unusually empty. MarketGrader has no public engineering blog,
no discoverable GitHub org, and job-posting searches (careers page, Indeed,
LinkedIn Jobs, ZipRecruiter, Built In) turned up zero current MarketGrader
listings — consistent with an ~11-15 person company that may not hire data
roles often enough to leave a public trail. One public reference to a
"Director of Software Engineering" role at the company surfaced but with no
JD text to mine for tools. **Do not present any tool in this table as
confirmed** — if the room names their actual stack, that supersedes
everything here immediately.

## Pain — confidence: low

No AE quotes exist. The one pain that's structurally implied by the
business itself: MarketGrader's entire commercial value — ratings people
trust enough to buy, indexes other firms license and build ETFs on — rests
on a daily scoring pipeline running across tens of thousands of tickers
with no visible discovery-note evidence of how they currently catch a bad
input before it reaches a published rating or index constituent list. That
is a plausible, not confirmed, point of leverage. Ask early in the room
what actually brought them to this call.

## Data domains

Fundamental/financial statement data (growth, value, profitability, cash
flow indicators) and market data (pricing, for sentiment-based shorter-term
signals mentioned on their site) across 39,000-41,000+ global public
companies. Cadence: daily (ratings recompute daily). Index
membership/rebalance cadence not publicly stated — treat as periodic
(commonly quarterly for GARP/quality indexes) unless corrected in the room.

## Public signals

- **Job postings:** None found on Indeed, LinkedIn Jobs, ZipRecruiter, or
  Built In as of this search. One public reference to a "Director of
  Software Engineering" title exists (via a general search snippet) but no
  live posting or JD was found to mine for tools. A company this size
  (~11-15 employees per LeadIQ) may simply not post data-role openings
  often — absence of postings here is not strong evidence of anything.
- **Engineering blog / GitHub:** None found under the MarketGrader name.
- **Recent news:** Nothing dated 2026 surfaced — no funding, acquisition,
  new exec hire, or platform-initiative announcement. MarketGrader Capital's
  most recent Form ADV (filed 2026-03-16) lists AUM as "None," which is
  notable but its meaning (wound-down managed AUM vs. an index-only
  licensing model with no discretionary AUM to report) is not established
  publicly — flag, don't interpret, in the room.
- **Industry / compliance:** MarketGrader Capital is a Florida-registered
  investment adviser; standard SEC/RIA recordkeeping and data-integrity
  expectations apply to anything that feeds published index constituents,
  but no specific compliance program was named anywhere.
- **Orchestration signals:** None. No Airflow, dbt, warehouse-migration, or
  platform-initiative signal of any kind turned up in 9 targeted web
  searches plus Drive/Gmail search.

## Conflicts and gaps

No AE notes exist to conflict with public data — the gap is total on the
technical side. The one thing worth flagging explicitly in the room rather
than assuming: MarketGrader Capital's AUM reads as "None" in its most
recent Form ADV despite the firm actively licensing indexes (including the
Barron's 400) to ETFs and institutions. That's either a normal artifact of
an index-licensing (vs. discretionary-management) business model, or a
signal the business has changed shape recently — don't guess which in the
brief or the room.

---

# Build directives

## Asset graph

Graph-first, small-to-medium. Given confidence is low across the board,
size this to a clean, legible story rather than a large enumerated list —
**8-10 assets**, matching the two real processes the public research
actually supports:

- **`ratings_ingestion` group** — `raw_fundamentals_feed` (daily,
  date-partitioned; external/observable-style asset representing the daily
  pull of fundamentals + market data for the ~40k-company universe — no
  vendor named, so keep this generic rather than badging a specific data
  provider), `raw_market_pricing_feed` (daily, same partition dimension).
- **`scoring` group** — `company_fundamental_scores` (the 24-indicator,
  0-100 GARP+Quality score per company, downstream of both raw assets),
  `company_ratings` (buy/hold/sell derived from the score — this is the
  asset a bad upstream input would corrupt, and the natural place for a
  blocking check).
- **`index_publication` group** — `index_constituents` (which companies
  are in each MarketGrader index this period, downstream of
  `company_ratings`), `barrons_400_constituents` (named explicitly — it's
  the one real, publicly branded index and makes a concrete money-shot
  target), `licensee_distribution_extract` (the file/feed that goes out to
  ETF sponsors/institutions licensing the index).
- **`reporting` group** — one rollup asset a research/ops persona would
  actually look at (e.g. `daily_coverage_summary` — how many of the ~40k
  companies scored cleanly today vs. flagged).

That's 8 assets; add a `sentiment_signals` asset in the `scoring` group
(MarketGrader's site explicitly also markets shorter-term
sentiment-based ideas alongside the fundamentals ratings) if time allows,
for 9. Do not scale this up further — with zero confirmed specifics,
a bigger enumerated graph would just be more confident-sounding guessing.

## Fidelity

**Stubbed (default) — graph-first.** Nothing here is data-backed. With no
AE notes and no confirmed volumes, computing real synthetic 24-indicator
scores across a fake 40k-company universe would be effort spent making up
numbers nobody asked for. Asset bodies are `pass`. If Carlos or the room
name real requirements live, that's an input to a future rebuild, not this
one.

## Demo shape

**Build-a-pipeline.** Nothing in public research suggests an existing
orchestrator to observe or a migration in progress — this reads as a
from-scratch build-a-transformation-graph story (their likely current
"orchestration" is probably cron/scripts, not a named platform worth
depicting). If the room reveals an existing tool (Airflow, cron, a
homegrown scheduler), that changes the shape toward
orchestrate-existing-workloads — note that possibility to Eric but do not
build for it speculatively.

## Native integrations to use

**None can be named with confidence.** No ingestion tool, warehouse, or BI
tool is confirmed. Per house rules, prefer generic over
specific-and-wrong: use `dg dbt`-free, warehouse-free scaffolding —
plain Python/asset-based ingestion and transform assets, DuckDB as the
(unbadged, or minimally badged) engine, no `kinds` badge claiming a
specific vendor stack. If literally nothing is named in the room either,
this stays generic; that is the correct outcome, not a gap to apologize
for.

## Community components to search for

Given the stack is entirely unconfirmed, search terms are exploratory only
— run these before assuming nothing fits, but expect this to land on
core Dagster (asset checks, freshness policies, partitions) rather than a
named integration:

- `financial data feed`
- `market data`
- `fundamentals`
- `index construction`

Do not force a registry component onto an unconfirmed integration surface
just to have one — an honest graph-first build with no named vendor is the
correct outcome here, not a rung-4 custom component.

## Asset checks

At least three, each mapped to the one structurally-implied pain (bad data
reaching a published, licensed index):

1. **`company_fundamental_scores` completeness — blocking.** Every company
   in the day's universe has a non-null score before ratings compute from
   it. This is the check that answers "how do you know your input data was
   right today," which is the single highest-leverage question anyone at
   MarketGrader could ask.
2. **`index_constituents` membership-change bounds — blocking.** Day-over-
   day constituent turnover for a given index stays within an expected
   band; a check that would catch, e.g., a scoring bug suddenly flipping
   hundreds of companies in or out of an index at once.
3. **`daily_coverage_summary` reconciliation — warning.** Flags if the
   count of companies scored today diverges meaningfully from the prior
   day's universe size, without blocking downstream (a legitimate
   universe-size change shouldn't halt the pipeline, just get flagged).

## Demo mode

- **What must be mocked:** Everything — no real fundamentals/market-data
  vendor credentials exist or are implied. Synthetic date-partitioned
  fixtures stand in for the daily fundamentals/pricing feed.
- **What can be real:** DuckDB for all storage; the asset checks, freshness
  policy, and automation condition logic are all real Dagster, running
  against the synthetic fixtures.
- **Asset kinds to display:** None badged with confidence — no vendor is
  confirmed for warehouse, ingestion, or BI. Leave `kinds` generic/absent
  rather than guessing `snowflake` or similar; this is a direct application
  of "prefer something generic over something specific-and-wrong."
- **Everything materializes green.** No planted failures, no staged
  anomaly. Checks, freshness, and automation are built, wired, and visible
  against a fully green graph; the pain narrative above is a talk track,
  not something the demo enacts live.
- **Data realism notes:** Keep the synthetic universe small (a few hundred
  companies, not 40,000) for a graph that materializes fast on a shared
  screen — say explicitly in the demo script that this is a scaled-down
  stand-in for their real ~40k-company universe, so nobody mistakes it for
  a claim about real scale.

## Three buckets

- **Implemented in code:** the 8-9 assets above, the 3 asset checks, one
  freshness policy on `company_ratings` (the asset a research/institutional
  persona would page someone about — "is today's rating current"), eager
  automation on the scoring→index_publication chain gated on the blocking
  checks, asset metadata (row/company counts, index names) on every asset.
- **Handled by Dagster+:** alerting (any check or freshness failure —
  demonstrate the policy UI, do not build notification code), restart-
  from-failure, run history/duration trends, lineage visualization, asset
  health.
- **Conversation only:** any real vendor integration (whichever
  fundamentals/market-data provider MarketGrader actually uses — unnamed,
  so nothing to build), any licensee-distribution/webhook integration to
  actual ETF sponsors, SSO/RBAC if it comes up given the RIA compliance
  context.

## Demo name

**"One Bad Number, One Wrong Index"** — names the actual stakes (a scoring
error flows into a licensed, real-money index) without overclaiming
anything about their internal architecture.

## Money shot

Materialize the graph green end-to-end, then click into
`barrons_400_constituents` and walk the lineage back through
`company_ratings` → `company_fundamental_scores` → `raw_fundamentals_feed`
in one view — "this is the real, publicly-tracked index your platform
publishes; here's every input that determined today's constituents, and
here's the check that would have stopped a bad one before it got this
far." That's a story that lands whether or not anything else in this brief
turns out to be accurate, because it's about their own named product
(Barron's 400), not a guessed internal detail.

## Capability talk track

- **Blocking checks on `company_fundamental_scores` and
  `index_constituents`:** "if a data vendor sends a garbled or missing
  value for even one of 40,000 companies today, this stops the bad score
  or the bad constituent list from ever reaching something you or a
  licensee acts on — instead of finding out from a client."
- **Freshness policy on `company_ratings`:** "you'd know within minutes if
  today's ratings didn't update, not when a customer or licensee asks why
  the numbers look stale."
- **Eager automation:** "add a new index tomorrow and it's declarative
  config wiring existing assets, not a new pipeline to hand-schedule."
- **Dagster+ alerting (platform, not built here):** point at the alerting
  policy UI — "this is where the check failure above would page whoever
  owns data quality, no custom code."
- **Restart-from-failure (platform, not built here):** "if the fundamentals
  feed partially fails mid-run, you re-run just the failed partition, not
  the whole day's 40,000-company job."

## Explicitly out of scope

- Any named vendor integration (fundamentals data provider, warehouse, BI)
  — nothing is confirmed, so nothing is built against a guessed vendor.
- Real/data-backed scoring logic — stubbed only, per Fidelity above.
- Index rebalance-cadence modeling (quarterly vs. other) — not confirmed,
  so the demo models daily scoring only and doesn't attempt to depict a
  specific rebalance schedule.
- Anything from the "Diez / Carlos" personal-name signal beyond confirming
  he's likely the primary contact — no further inference about his
  priorities beyond what's in this brief.
