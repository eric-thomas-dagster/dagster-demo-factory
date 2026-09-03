# DEMO_SCRIPT — "From Ran to Right" (RVU / Tempcover)

90-minute technical deep-dive. Lisa Smith (built the current Azure stack)
and Iain Millar (technical champion, one month in, already advocating for
Dagster over Cloud Composer) are in the room. Tom (VP Engineering, holds
the budget decision) is not — this call's job is to give Iain a concrete
technical argument to hand to Tom, not to convince the room itself.

## 0. Open with Iain's own words

Say: *"You told us Azure Data Factory can tell you a job ran, but not
whether the data coming out of it was good. That's the whole demo — I'm
going to show you the difference."*

## 1. The money shot — the full graph, green

1. Open the asset graph in `dg dev` (http://localhost:3000). Group by
   `ingestion` / `dbt` / `activation` / `reporting` so Fivetran → dbt →
   Braze/Power BI reads left to right.
2. Point out the ingestion layer is badged `fivetran` (their real, named
   tool), the dbt layer `dbt`+`bigquery` (their two locked decisions), and
   downstream `braze`/`powerbi`.
3. Materialize everything. Say: *"Every asset here is real — the ingestion
   layer runs through Dagster's actual Fivetran integration, the reporting
   asset through the actual Power BI integration, and the dbt layer is
   running real dbt Core against real rows, with real tests. Nothing in
   this graph is hardcoded to pass, and nothing here is a stand-in — this
   is the same code path that runs against your live Fivetran account and
   BigQuery warehouse once we swap in your credentials."*

## 2. Click `fct_bound_policies_daily`

1. Open its metadata panel. Point at the **freshness policy** and the
   passing **checks** in the same panel — dbt-derived tests plus the custom
   reconciliation check.
2. Say: *"If this check had failed — say the panel feed went quiet and the
   aggregation silently dropped rows — only this one materialization would
   be blocked. Not the whole job."* This directly reverses Iain's own line:
   *"any pipeline failure requires disassembling, fixing, and rerunning the
   entire job."*
3. Click the lineage back through `stg_bound_policies` and
   `stg_panel_feed` to `raw_bound_policies` / `raw_panel_insurer_feed`.
   Say: *"Every number downstream has a visible, checked path back to the
   Fivetran sync it came from."*

## 3. Click into the dbt project directly

1. Open `stg_bound_policies`'s metadata panel — show the raw SQL and the
   auto-surfaced `not_null`/`unique` tests. Say: *"This is your actual dbt
   project. We didn't build a separate testing framework on top of it —
   your tests are the Dagster checks."*
2. Say: *"This dbt project runs against DuckDB today because we don't have
   your BigQuery credentials. Flipping to your real warehouse is a
   `profiles.yml` target change — same models, same tests, same graph."*

## 4. Branch deployments against BigQuery — Iain's own mechanical question

1. Open a second, empty branch deployment in Dagster+ pointed at the same
   project.
2. Walk through how it gets its own schema against the same BigQuery
   project. Say: *"This is the specific question you asked — how do
   ephemeral environments work when there's only one real BigQuery
   instance. Each branch gets its own schema; nothing collides with prod."*
3. This is the direct answer to *"changes cannot be tested safely before
   hitting production"* — one of Lisa's named pains.

## 5. Automation and schedule

1. Open any dbt-layer asset (e.g. `stg_quote_requests`). Show its `eager`
   automation condition. Say: *"The moment a new Fivetran sync lands, this
   recomputes on its own — no cron job to remember."*
2. Open `stg_panel_feed` specifically. Say: *"This one is gated — it only
   auto-recomputes once the panel feed's completeness check passes. If a
   sync comes in with fewer than seven insurers reporting, propagation
   stops here, not three steps downstream."*
3. Point at `rvu_morning_marts_schedule` in the Schedules tab.

## 6. Fivetran's own schedule, observed

1. Open the Sensors tab and point at
   `fivetran_rvu_demo_account__sync_status_sensor`. Say: *"Fivetran syncs
   on its own schedule too — this polls for syncs Fivetran ran that Dagster
   didn't trigger, and folds them into the same lineage graph as an
   observation. You don't lose visibility just because something else
   started the run."*

## 7. What Dagster+ adds on top (don't build it live — point at it)

Say: *"None of what's coming next is code I wrote for this demo — it's
just there:"*

1. **Restart-from-failure and run history** — replaces the bespoke
   warehouse logging code currently eating engineering time. The direct
   answer to *"any pipeline failure requires disassembling, fixing, and
   rerunning the entire job."*
2. **Native alerting** (Slack/email/PagerDuty) on check failures, freshness
   violations, or run failures — no hand-rolled Slack sensor.
3. **RBAC and viewer licenses** — how Tom's business stakeholders get
   visibility without edit access, without a second project.
4. **Insights dashboard** — observability over time, once this is running
   in production.

## 8. Close

Say: *"Everything you saw today — the dbt tests, the freshness policy, the
lineage, the Fivetran and Power BI integrations — works identically the
day this points at your real Fivetran account, Power BI workspace, and
BigQuery project instead of local simulations. That's the whole point of
building it this way: nothing about the orchestration logic changes when
the credentials do."*

Then, to Iain directly: *"You've got the technical case now. What do you
need from me to help you make the cost case to Tom?"*

## What's out of scope today (say if asked, don't volunteer)

- No dbt Semantic Layer or structured LLM data access — dbt's own roadmap,
  not something Dagster builds; mention it enables the AI ambitions once
  the platform is modernized.
- No self-hosted-on-GKE walkthrough — Dagster+ Hybrid is the middle
  ground between full SaaS and self-hosting; described, not built.
- No assets for RVU's other four brands (Uswitch, Confused.com,
  money.co.uk, Mojo Mortgages) — this rebuild is scoped to Tempcover.
