# DEMO_SCRIPT — "Grid at Scale" (E.ON Sverige)

60-minute "Demo/Deeper Dive" call with nine E.ON attendees (titles not
exposed by the calendar invite) — a large, cross-team room for a first
look. **No live discovery notes exist for this prospect.** Lead with the
public investment/regulatory hook, not a claimed quote from the room, and
say plainly, if asked, that the stack specifics below are directional, not
confirmed.

## 0. Open with the hook (before touching the screen)

Say: *"You're mid-way through your largest-ever Swedish grid investment and
rolling out about a million new smart meters. That's a step-change in the
volume and criticality of the telemetry flowing through whatever you
orchestrate today — and at the same time, the new EU rule on
customer-switching data (2026/855) means you need a provable, auditable
trail for that specific data. I built a scenario that shows both: absorbing
the volume, and getting the audit trail as a side effect of the same
lineage graph."*

## 1. The money shot — the full graph, green

1. Open the asset graph in `dg dev` (http://localhost:3000). Group by
   `metering_ingestion` / `grid_quality` / `customer_switching_compliance`
   / `grid_reporting` so the layering reads left to right.
2. Materialize everything (**Materialize all** in the UI, or see the
   README's per-chain launch commands for the CLI equivalent — `raw_meter_reads`
   is multi-partitioned by date x grid zone, so it needs a partition key
   like `2026-08-26|se3`). Say: *"This all runs green — today's point isn't
   to show you a failure, it's to show you the safety net that's there when
   something does break in production."*

## 2. Click `raw_meter_reads`

1. Open its metadata panel. Point at the **partition** dimensions: date x
   grid zone. Say: *"Your meter rollout means this asset is about to see
   several times the volume it sees today, per zone, per day. This is the
   asset that earns a two-dimensional partition because of that — you could
   backfill or investigate a single zone on a single day without touching
   anything else."*
2. Click into `meter_reads_completeness_check`. Read its description aloud.
   Say: *"This is wired and passing today. In production, this is the one
   that tells you, per zone, the moment a batch of meters stops reporting —
   before it becomes a grid-operations problem."*

## 3. Click `grid_load_hourly`

1. Open its metadata panel. Point at the **freshness policy** badge.
2. Say: *"This is the asset your grid-operations team would page someone
   about. If load telemetry goes stale or a reading is physically
   implausible, this goes red before it reaches anything downstream — and
   you'd see exactly which upstream feed caused it."*

## 4. Click `customer_switching_extract` → `switching_data_audit_log`

1. Open `customer_switching_extract`'s metadata. Point at `business_impact`
   — the EU 2026/855 framing.
2. Click into `switching_extract_audit_completeness_check`. Read its
   required-fields list aloud (`produced_by_run_id`, `produced_at`,
   `source_partition`, `upstream_check_status`).
3. Open `switching_data_audit_log` and show its automation condition: eager,
   but gated on the extract's blocking check passing.
4. Say: *"This is the audit trail the new EU rule asks for — who produced
   this switching-data extract, when, and against which check results —
   and it's free, because it's the same asset-level metadata you'd want
   anyway. It never rebuilds off an extract that failed its own audit
   check."*

## 5. Automation and the schedule

1. Point at the **Schedules** tab: `eon_daily_telemetry_ingestion_schedule`,
   03:00 CET.
2. Say: *"Grid-load telemetry lands on a schedule timed two hours ahead of
   its own freshness deadline. And every downstream asset in this graph
   rebuilds itself declaratively the moment its input lands and passes its
   checks — nobody has a cron job to remember."*

## 6. What Dagster+ adds on top (don't build it live — point at it)

Say: *"Three things I didn't write a single line of code for, because
they're just there:"*

1. **Alerting** — if a check like the ones we just looked at ever failed,
   Dagster+ can Slack or email your team automatically.
2. **Restart-from-failure** — if a run fails partway, you restart from the
   failure point, not from scratch.
3. **Run history and lineage** — every run of this graph, ever, is here.

## 7. Close

Say: *"I'll be straight with you: I don't yet know what you actually run
today — no discovery call has happened before this one, so everything
you're looking at uses generic names and a synthetic graph. But every piece
of this — the checks, the freshness policy, the automation — works
identically the day we point it at your real metering platform, your real
grid-telemetry feed, your real switching-data system. That's the whole
demo-mode idea: same graph, same checks, real credentials instead of
none."*

If the call surfaces real system names, a confirmed use case, or a clearer
picture of who's in the room and why, say so explicitly — that's the
fastest way to turn this into a build worth another hour.

## What's out of scope today (say if asked, don't volunteer)

- No dbt, no Snowflake, no Fivetran, no Databricks — none confirmed for
  E.ON specifically.
- No real Landis+Gyr/Gridstream Connect API shapes, no real SAP IS-U or
  grid-operations system names.
- No specific EU 2026/855 regulatory-reporting format — modeled only as
  "an extract with audit metadata," not a real compliance deliverable.
- Why the room has nine attendees across `eon.se`/`eon.com`/`.external`
  addresses — genuinely unknown; ask if it comes up naturally.
