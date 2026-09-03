# DEMO_SCRIPT — "One BI Team, Two Companies" (Noleggiare / FTH Group)

60-minute technical deep-dive with Raffaele Vumbaca (BI Manager, Finance,
Noleggiare — the decision-maker) and his shared BI team, plus one Tomasi
Auto attendee. Raffaele has already run Dagster Starter for three months
and built a real migration workflow and an ML pipeline on it — **this is an
architecture review, not a first-look demo.** Lead with an opinionated
recommendation, not a feature tour, per the AE notes ("mainly wants to see
what the SE recommends").

## 1. Open with the shape of the recommendation, not a screen

Say: *"You've already proven Dagster works for you — three months in, with
a real migration workflow and an ML pipeline built yourselves. What I want
to show today is how this scales to both companies under one platform, and
answer the two things I think you're actually deciding: how a two-company
BI team stays visibly separate but centrally governed, and what it costs
you to move off Postgres later. Let's look at the graph."*

## 2. The money shot — the full graph, green

1. Open `dg dev` (http://localhost:3000), group by
   `noleggiare_rental_ops` / `tomasi_dealer_ops` / `shared_finance_warehouse`
   / `ml_workflows` / `bi_publish` / `future_state_snowflake`.
2. Point out `noleggiare_rental_ops` and `tomasi_dealer_ops` as visually
   distinct groups, both feeding one `shared_finance_warehouse` — and
   `fleet_residual_value_forecast` sitting inline in that same lineage,
   not off in a side project.
3. Materialize everything (**Materialize all**). Say: *"This all runs
   green — the point today is the safety net that's there when something
   breaks in production, not a staged failure."*

## 3. Click `fact_finance_consolidated_daily` — the structural answer

1. Open its metadata panel. Show the **date x company partition grid** —
   click into a Noleggiare partition, then a Tomasi Auto partition.
2. Say: *"This is one asset, materialized per company. In Dagster+, RBAC is
   what keeps a Tomasi Auto user from touching Noleggiare's partitions
   without you needing two separate projects — one asset graph, two
   governed slices."*
3. Point at the passing **blocking** check and the **freshness policy**
   badge. Say: *"This is the data-quality-and-alerting layer you told us
   was next on your list, after the migration and ML work you've already
   done yourselves."*

## 4. Click over to `fact_finance_consolidated_daily_snowflake`

1. Open its metadata panel side by side (mentally) with the Postgres
   version. Say: *"Same lineage, same deps, different warehouse resource —
   this is the whole Postgres-to-Snowflake conversation, and it's a config
   change, not an orchestration rewrite. Whichever of you two ends up
   needing Snowflake first, this is what that migration looks like in the
   graph."*

## 5. Click `dim_vehicle` — the cross-company check

1. Open its blocking `dim_vehicle_cross_company_consistency` check. Read
   its description aloud.
2. Say: *"This is the single check that most directly answers 'how do two
   companies coordinate around a shared platform' — it catches a VIN
   reported as active fleet inventory and active dealer inventory at the
   same time, which is exactly the kind of double-count that happens when
   two teams run separate scripts against the same underlying vehicles."*

## 6. Click `fleet_residual_value_forecast`

Say: *"This sits in the same lineage as the ETL layer, not off in a
notebook somewhere — the same pattern you've already started building
yourselves. ML and ETL coexist in one graph, one set of checks, one
freshness story."*

## 7. Automation and schedule

1. Open any `shared_finance_warehouse` asset (e.g. `dim_vehicle`). Show its
   `eager` automation condition. Say: *"New source data flows through
   automatically — no cron job either team has to remember exists."*
2. Point at `noleggiare_morning_ingest_schedule` in the Schedules tab. Say:
   *"The raw ingestion and per-company facts run on a fixed morning
   schedule; everything downstream of them — the consolidated fact, the
   forecast, the Qlik export — recomputes automatically once they land.
   That's the schedule-plus-declarative-automation combination."*

## 8. Click `qlik_cloud_export`

Say: *"This is the one place in the graph with a real network boundary —
right now it's writing a simulated publish manifest because I don't have
your Qlik Cloud credentials. Flip `demo_mode: false` in one line of YAML,
add your tenant URL and API key, and this calls your real Qlik Cloud
reload API. Nothing else in the graph changes."*

## 9. What Dagster+ adds on top (don't build it live — point at it)

Say: *"A few things I didn't write a single line of code for, because
they're just there:"*

1. **RBAC / permissions** — direct answer to the two-company governance
   question: scope access so each company's team sees and acts on their
   own pipelines, under one deployment you administer centrally.
2. **Alerting** — if a check like the ones we just looked at ever failed,
   Dagster+ can Slack or email your team automatically. Native policy, not
   a custom script.
3. **Restart-from-failure, run history, lineage visualization, asset
   health** — every run of this graph, ever, is here.
4. **Hybrid-on-AWS deployment** — since you're already on AWS and asked
   about this: describe the agent architecture (Dagster+ control plane,
   your own AWS-hosted agent executing runs, no data leaving your VPC) and
   point at the docs. Don't fake a deployment against their AWS in this
   call.

## 10. Close

Say: *"Everything you saw today — the checks, the freshness policy, the
automation, the two-company partitioning — works identically the day you
point the Postgres resources at your real database and the Qlik export at
your real tenant. That's the whole demo-mode idea, and it's the same
pattern behind the migration workflow and ML pipeline you've already built
yourselves."*

## What's out of scope today (say if asked, don't volunteer)

- No dbt — not evidenced for this prospect, despite the Tomasi Auto job
  posting naming Talend/Pentaho/Azure Data Factory as separate ETL tools.
- No real Qlik Cloud, Snowflake, or AWS credentials — everything demo-mode
  mocked.
- No TC8/Forthing (the third FTH Group entity) — mention only if it comes
  up; nothing built for it.
- No specific rental/dealer source-system product names — generic,
  domain-plausible names only; none confirmed in the AE notes.
