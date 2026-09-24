# DEMO_SCRIPT.md — "One Index, End to End"

MarketGrader.com — 2026-09-25, 11:30 AM–12:30 PM America/New_York. Technical
deep-dive following the discovery call. Room: Jim Andrews (Director of Index
Solutions, technical champion), Carlos Diez (CEO, final decision-maker,
non-technical, worried about implementation time/downtime), Pablo Delgado
(Eng Director, silent so far, buy-in unwon), Juan Be and Sabrina Muller (both
quiet on discovery).

**Run this locally with `dg dev`, not against the Dagster+ deployment** — see
README's "Demo mode" table for why (Serverless storage is ephemeral).

**Before the room:** get the Prefect-merger answer from the internal team —
Jim's question on the discovery call went unanswered. Not a build item; see
brief's "Conflicts and gaps."

---

## 1. Open on the whole graph (30 sec)

`dg dev` → Asset graph, no filters. Everything green.

> "This is the state of MarketGrader's data platform mid-migration — your
> current SQL Agent/SSIS estate and the new Iceberg-based pipeline, in one
> view."

## 2. Point at the legacy estate (1 min)

Click into the `legacy_estate` group —
`legacy_sql_agent_price_ingest` / `legacy_sql_agent_index_calc`. Note the
dependency arrow between them.

> "These two represent every index you haven't migrated yet. Dagster now
> triggers and polls these SSIS packages directly — same packages,
> replacing SQL Agent as the scheduler. Your notes said these run serially;
> that's now a real dependency in the graph, not just a description of
> today's scheduler." Point at the `integration_pattern:
> dagster_calls_legacy` metadata on the asset.

Materialize the `legacy_estate` group live — both packages execute in one
run, in order, and go green fast (demo mode). Then click the sensor
(`ssis_workspace_observation_sensor`, currently stopped by default) and
explain it still catches anything triggered outside Dagster — "if someone
kicks off that SSIS package by hand at 2am, Dagster still sees it."

## 3. Materialize the new pipeline (2 min) — the money shot setup

Select `raw_price_feed` → `raw_corporate_actions` → `barrons_400_constituents`
→ `licensee_distribution_extract` → `daily_coverage_summary`, materialize
today's partition.

Everything goes green. Click into `raw_price_feed`:

> "This writes real Iceberg tables — not a database pretending to be
> Iceberg. Same format your target Polaris catalog speaks. Flip one line in
> YAML and point this at your real lake."

## 4. The checks (2 min) — discovery question #1

Click `raw_price_feed`'s asset check (`raw_price_feed_arrival`) and
`raw_corporate_actions`'s (`raw_corporate_actions_arrival`).

> "A late or garbled price feed or corporate action stops here, before it
> reaches a published, licensed index constituent list. You find out here —
> not from a client asking why the index looks wrong." Blocking severity:
> point out `barrons_400_constituents` would refuse to compute if either
> failed.

Click `barrons_400_constituents_membership_bounds` (warning severity):

> "This one catches a scoring bug — hundreds of constituents flipping
> overnight — without blocking a legitimate rebalance."

## 5. Freshness + automation (1.5 min)

Click `barrons_400_constituents`'s freshness policy.

> "You'd know within minutes if today's index didn't recompute, not when a
> licensee asks."

Point at the automation condition (eager, gated on the two blocking checks).

> "The index recomputes the moment its inputs land — not on a fixed
> schedule, not something someone has to remember to kick off. And it won't
> fire on incomplete input."

## 6. THE MONEY SHOT — the lineage view, for Carlos (2 min)

Zoom out to the full graph again. Walk the line from `legacy_sql_agent_*`
sitting alongside `raw_price_feed → raw_corporate_actions →
barrons_400_constituents → licensee_distribution_extract`.

> "This is what a phased cutover looks like. The indexes you haven't
> migrated yet keep running the exact same SSIS packages as today — Dagster
> just took over scheduling them, so you already get lineage, checks, and a
> single pane of glass on the whole estate before a single line of SSIS
> changes. This one index is now fully on the new platform. Nothing about
> migrating index two requires you to touch index one, or SQL Agent."

This is the direct answer to Carlos's stated fear (implementation time and
downtime) and Jim's discovery question #2 (seeing legacy and new in one
lineage view).

## 7. Restart-from-failure (1 min) — discovery question #3, Dagster+ capability

Don't fail anything live. Narrate against the green graph, pointing at any
asset:

> "If the person who normally reruns a failed index calc is out, anyone with
> access sees exactly which asset failed and reruns just that one step. No
> tribal knowledge required." Mention this is native Dagster — nothing
> custom-built.

## 8. Alerting + RBAC/audit (Dagster+, Pro tier) (1 min)

Point at (don't demo live unless time allows) the alert-policy UI and
RBAC/audit trail.

> "This is where a check failure pages whoever owns data quality — no custom
> code. Same mechanism the missing-price-file scenario in your notes would
> trigger." For Carlos/finance: "RBAC and audit trail are native to Pro —
> this is the real answer to the tier question."

## 9. Branch deployments / catalog (30 sec, if time)

One-line mention per the AE's own suggested flow step 5. No live demo
required.

## 10. Close

> "This is 'One Index, End to End' — the exact scope Jim proposed. Migrate
> one index fully, prove it, keep going. Nothing here required a big-bang
> cutover, and nothing here is scaffolding — every piece you saw is either
> real Dagster capability or your actual data flow."

---

## If asked live

- **"Does this actually run against our SQL Server / Polaris?"** — flip
  `demo_mode: false` in `defs/legacy_estate/defs.yaml` /
  `defs/resources/defs.yaml`, supply real credentials. Same component, same
  graph, same checks — see README's demo-mode table.
- **"How big can this scale?"** — point at the workspace-component shape:
  adding MarketGrader's next legacy package or lakehouse table is one more
  YAML entry, not another component instance.
- **Prefect merger** — do not answer live; Eric has the answer prepared
  separately.
