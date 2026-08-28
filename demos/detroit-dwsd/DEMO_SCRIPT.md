# DEMO_SCRIPT — "One Warehouse, Every Pipe" (City of Detroit DWSD)

30-minute intro call. Anthony Urbina (DWSD Applications Analyst II,
Integrations) is the only DWSD attendee — keep this legible in a first
look, not dense. No live discovery notes exist for this prospect, so lead
with the posting-language hook, not a claimed quote from the room.

## 0. Open with the hook (before touching the screen)

Say: *"Your team's own job posting for the Integrations Analyst 2 role asks
for someone to hand-code T-SQL, PL/SQL, Java, and Python integrations from
your legacy, web, cloud, and packaged-app systems into your warehouse — and
it doesn't mention an orchestrator, a lineage tool, or a checks layer
anywhere. So I built exactly that scenario to show you what governing it
would look like."*

## 1. The money shot — the full graph, green

1. Open the asset graph in `dg dev` (http://localhost:3000). Group by
   `dwsd_ingestion` / `dwsd_warehouse` / `dwsd_reporting` so the layering
   reads left to right.
2. Point out the four ingestion sources are badged `sqlserver` and `oracle`
   — the exact stack named in the posting, not a generic icon.
3. Materialize everything (**Materialize all** in the UI, or
   `dg launch --assets '*' --partition <today's date>` for the partitioned
   half plus a second unpartitioned launch — see README for the two-chain
   split). Say: *"This all runs green — the point today isn't to show you a
   failure, it's to show you the safety net that's there when something
   does break in production."*

## 2. Click `water_quality_compliance_daily`

1. Open its metadata panel. Point at the **freshness policy** badge and the
   passing **blocking check**.
2. Say: *"If this were late or incomplete, this would be red before it ever
   reached a compliance report — and you'd see exactly which upstream
   extract caused it."*
3. Click into the check itself. Read its description aloud — it names the
   exact required reading types it enforces (`chlorine_residual`,
   `turbidity`, `coliform`, `lead_and_copper`). Say: *"This is wired and
   passing today; in production, this is the one that would catch a missing
   lab reading before it becomes a late EPA filing."*

## 3. Click `meter_reading_extract`

1. Open its metadata panel. Point at `owner`, `sla`, and `business_impact`
   — all filled in. Say: *"This isn't a diagram. Every node here carries who
   owns it, what breaks if it's late, and what we'd expect from it."*
2. Point out the **partition** on this asset (daily). Say: *"Your posting
   calls out 'high frequency data loads' — this is the asset that earns a
   partition because of that; you could backfill or investigate a single
   day without touching the rest."*

## 4. Automation — the recompute story

1. Open `compliance_reporting_extract`. Show its automation condition:
   eager, but gated on `water_quality_compliance_daily`'s blocking check
   passing.
2. Say: *"This is Dagster's declarative automation — the regulator-facing
   extract rebuilds itself the moment new compliance data lands and passes
   its check. Nobody has a cron job to remember, and it never rebuilds off
   data its own upstream check has already flagged as incomplete."*
3. Point at the `dwsd_daily_ingestion_schedule` in the Schedules tab. Say:
   *"And the two source extracts run on a schedule timed to a real
   deadline — two hours ahead of when the downstream warehouse needs
   them."*

## 5. What Dagster+ adds on top (don't build it live — point at it)

Say: *"Three things I didn't write a single line of code for, because
they're just there:"*

1. **Alerting** — if a check like the one we just looked at ever failed,
   Dagster+ can Slack or email your team automatically. Native policy, not
   a custom script.
2. **Restart-from-failure** — if a run fails partway, you restart from the
   failure point, not from scratch.
3. **Run history and lineage** — every run of this graph, ever, is here,
   and the lineage view is exactly what we've been looking at.

## 6. Close

Say: *"Today's example uses generic names because I don't have your actual
system names yet — but every piece of this (the checks, the freshness
policies, the automation) works identically the day we point it at your
real billing system, your real meter feed, your real lab data. That's the
whole demo-mode idea: same graph, same checks, real credentials instead of
none."*

If the call surfaces real system names or a confirmed use case, say so
explicitly — that's the fastest way to turn this into a build worth another
30 minutes.

## What's out of scope today (say if asked, don't volunteer)

- No dbt, no Airflow — neither is confirmed for DWSD specifically (a
  different city department runs Airflow+dbt for an unrelated open-data
  portal).
- No specific vendor product names for DWSD's real billing/meter/work-order/
  lab systems — none are confirmed publicly.
- No police/CJIS/ShotSpotter data — different department, unrelated, and
  not worth raising unprompted.
