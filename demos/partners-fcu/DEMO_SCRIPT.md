# DEMO_SCRIPT — "Dependencies, Not Duct Tape" (Partners FCU)

1-hour technical deep-dive / "Pro vs. OSS" comparison. Jason Jasmin (BI
technical lead, ran the original OSS pilot, main point of contact), Mark
Smith (Enterprise Architect, ex-Airflow), Srinath Sesham (Data Architect),
and Len (implementation consultant, Blue Coconut Strategies) are in the
room. This is a re-engagement after a stalled pilot, not a first look —
the job is to answer, specifically, why paying for Dagster+ beats what
they already tried and let stall.

## 0. Open by naming the three asks back to them

Say: *"Last time, alerting, quality checks, and partitioned assets were
the three things you said you needed to see to make this decision. That's
exactly what today is — nothing more, nothing less."*

## 1. The asset graph — their real source, their real vocabulary

1. Open the full lineage graph. Point out the `ingestion` group is badged
   `mssql` — their real, currently-running SQL Server, not a guess.
2. Say: *"`raw_member_transactions`, `raw_deposit_accounts`,
   `raw_loan_originations` — this is your vocabulary, not ours. And every
   one of these is daily-partitioned."*
3. Click `raw_member_transactions`. Open its Partitions tab. Say: *"This
   is the partitioned-assets story you asked for — each day is tracked,
   materialized, and checked independently. You can re-run one day without
   touching the rest."*

## 2. The money shot — blocking check, then live alerting

1. Click `raw_member_transactions`'s metadata panel. Point at
   `raw_member_transactions_completeness` — passing, blocking.
2. Say: *"You told us you're always building workarounds to get jobs to
   run in the right order and have dependencies running properly. This is
   the direct reversal: if this check goes red, `stg_member_transactions`
   and everything downstream of it — 8 more assets — refuse to compute
   automatically. Not silently. Not a workaround. A hard stop, in the
   product, with no code from your side."*
3. Switch to Dagster+. Open the alerting policy screen. Wire
   `raw_member_transactions_completeness` to a Slack channel, live, on
   screen. Say: *"This is the one thing you told us was untested in the
   OSS pilot. Here it is, configured in under a minute, no code."*

## 3. dbt — real project, real tests, zero extra plumbing

1. Click `stg_member_transactions`. Open its metadata panel — show the raw
   SQL and the auto-surfaced `not_null`/`unique` tests.
2. Say: *"This is your actual dbt Core project, running for real against
   this graph. We didn't build a separate testing framework on top of
   it — your tests are the Dagster checks. That's the direct answer to
   'quality checks' — no extra plumbing to maintain."*
3. Click through the lineage from `fct_daily_member_activity` back to
   `stg_member_transactions` to `raw_member_transactions`. Say: *"Every
   number downstream has a visible, checked path back to the SQL Server
   table it came from."*

## 4. Freshness and automation — how you'd know something broke

1. Open `fct_daily_member_activity`'s metadata panel. Point at the
   **freshness policy**. Say: *"This is the direct answer to 'how would we
   know something broke' — if this hasn't recomputed in 24 hours, it goes
   red on its own, with no cron job watching it."*
2. Open `stg_member_transactions`'s automation condition. Say: *"This
   recomputes the moment new data lands — and specifically, it's gated on
   the completeness check passing. A quiet SQL Server feed doesn't
   silently propagate a partial day forward."*
3. Point at `partners_fcu_morning_marts_schedule` in the Schedules tab.

## 5. What Dagster+ adds on top (don't build it live — point at it)

Say: *"Everything from here forward is not code I wrote for this demo —
it's just there, and it's the actual answer to 'why pay for this instead
of self-hosting.'"*

1. **Restart-from-failure and run history** — the mechanical fix for
   needing manual job-ordering workarounds today.
2. **Branch deployments** — dev/test isolation before touching production
   member data. Tie this explicitly to NCUA/GLBA vendor-risk expectations:
   *"Changes get tested against an isolated schema before they ever touch
   real member data — that's a concrete answer for your own vendor-risk
   committee, not just an engineering nicety."*
3. **RBAC and viewer licenses, run history, asset health and lineage UI** —
   say directly: *"This is what a year of self-hosting the OSS version
   didn't give you. It's not a feature gap in the open-source project —
   it's the operational layer around it that Dagster+ is."*

## 6. Close

Say: *"Everything you saw today — the SQL Server integration, the dbt
tests, the freshness policy, the lineage — works identically the day this
points at your real SQL Server instance and a real Snowflake warehouse
instead of local DuckDB. Flipping SQL Server on is one line plus real
credentials. Flipping Snowflake on is a `profiles.yml` target change nobody
in this room needs to write."*

Then, to Jason directly: *"Last time this stalled on priorities, not on
the product. What do you need from this meeting to make sure it doesn't
stall a second time?"*

## What's out of scope today (say if asked, don't volunteer)

- No BI tool build — none is named for Partners FCU; don't invent Power
  BI, Tableau, or Looker if asked what the reporting layer connects to.
- No real Snowflake — the warehouse badge reflects "likely adding," not a
  decided target; be direct that this is DuckDB standing in for it today.
- No SSIS or legacy-scheduler modeling — the pain described is ad hoc
  workarounds, not a named external orchestrator to observe.
- No planted failure anywhere — every check, freshness policy, and
  automation condition shown is real and currently green; the alerting and
  restart-from-failure story is told entirely through the Dagster+
  walkthrough and talk track above, never a staged break.
