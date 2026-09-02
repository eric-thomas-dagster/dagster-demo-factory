# DEMO_SCRIPT — "One Ledger, Every Desk" (Trafigura Group)

30-minute slot, audience unconfirmed (see README — verify this is a live
customer call before using this). No discovery notes exist for this
prospect, so lead with the public "why now," not a claimed quote from the
room.

## 0. Before touching the screen — confirm the room

Check who's actually attending. If no Trafigura person is present, this is
a rehearsal — walk through the script as if presenting, but don't claim any
line as something Trafigura said.

If it is a live call, open with: *"There's no single orchestrator named
anywhere in your public materials, and the one stack we could confirm — AWS,
Redshift, Glue, Power BI — is scoped to your Gas & Power desk specifically.
So I built a generic version of what a commodity-trading pipeline looks like
governed end to end, to show what that would look like across any desk."*

## 1. The money shot — the full graph, green

1. Open the asset graph in `dg dev` (http://localhost:3000). Group by
   `market_data` / `trade_capture` / `risk_warehouse` / `reporting` so the
   layering reads left to right.
2. Point out the ingestion layers are badged `s3`, the warehouse layer
   `redshift`, and the dashboard `powerbi` — matching the one confirmed
   stack, not a generic icon.
3. Materialize everything (**Materialize all** in the UI). Say: *"This all
   runs green — the point today isn't to show you a failure, it's to show
   you the safety net that's there when something does break in
   production."*

## 2. Click `fact_credit_exposure_daily`

1. Open its metadata panel. Point at the **freshness policy** badge and the
   passing **blocking check**.
2. Say: *"If this exposure figure were late or off by more than a rounding
   error, this would be red before a credit desk ever traded against it —
   and you'd see exactly which upstream trade or price feed caused it."*
3. Click into `fact_credit_exposure_daily_reconciliation`. Read its
   description aloud. Say: *"This is wired and passing today; in
   production, this is the one that catches an exposure figure that's
   drifted from the trades it's supposed to represent."*
4. Click the lineage back through `fact_trade_position_daily` to
   `trade_capture_raw`. Say: *"Every number on the dashboard downstream has
   a visible, checked path back to where it came from — that's the whole
   pitch."*

## 3. Click `trade_capture_raw`

1. Open its metadata panel. Point at `owner` and `business_impact`. Say:
   *"Every node here carries who owns it and what breaks if it's wrong —
   this isn't just a diagram."*
2. Open its blocking completeness check. Say: *"This is the one that would
   stop an unresolvable trade — missing a counterparty or commodity
   reference — from ever reaching a position or exposure figure."*

## 4. Automation and schedule

1. Open any `risk_warehouse` asset (e.g. `dim_counterparty`). Show its
   `eager` automation condition. Say: *"This is Dagster's declarative
   automation — the warehouse layer recomputes itself the moment upstream
   reference data changes. Nobody has a cron job to remember."*
2. Point at `trafigura_eod_risk_schedule` in the Schedules tab. Say: *"And
   the day's position and exposure figures compute on a schedule, so
   there's always a fresh EOD snapshot even without an upstream change to
   trigger it."*

## 5. What Dagster+ adds on top (don't build it live — point at it)

Say: *"Three things I didn't write a single line of code for, because
they're just there:"*

1. **Alerting** — if a check like the one we just looked at ever failed,
   Dagster+ can Slack or email the desk automatically. Native policy, not a
   custom script.
2. **RBAC** — relevant at your scale with distinct trading desks; each team
   sees and controls only what's theirs.
3. **Restart-from-failure and run history** — every run of this graph,
   ever, is here, and the lineage view is exactly what we've been looking
   at.

## 6. Close

Say: *"Today's example uses generic names because I don't have your actual
desk or system names yet — but every piece of this (the checks, the
freshness policy, the automation) works identically the day we point it at
your real Redshift account, your real trade capture feed, real credentials
instead of none. That's the whole demo-mode idea."*

If the call surfaces a real desk, system name, or confirmed use case, say so
explicitly — that's the fastest way to turn this into a build worth another
30 minutes.

## What's out of scope today (say if asked, don't volunteer)

- No dbt, Snowflake, or Databricks — none confirmed for this scope.
- No oil or metals & minerals desk specifics — only Gas & Power has any
  public stack evidence, and this build is deliberately desk-agnostic.
- The 2023/2024 fraud incidents and the new CIO's governance mandate —
  real, public, and part of why lineage/checks may resonate here, but not a
  script line inside the demo itself.
