# DEMO_SCRIPT — "Untangling the Spiderweb" (Umicore)

**No meeting is booked for this brief.** This script assumes a follow-up
technical deep-dive or architecture/budget review, per the AE's own
business-case document (built entirely from an internal doc, not fresh
discovery notes — the last confirmed live demo was August 5, 2026). Wim
Hendrijks (primary contact), Wauter, and Anthony are named participants
from prior discovery; titles are not confirmed. Adjust framing once a real
invite exists.

## 0. Open by naming the coexistence question back to them

Say: *"The question on the table isn't whether Dagster is technically
capable — it's whether a data mesh built from four independently-owned
business units can get one lineage graph and event-based recovery without
displacing the Stonebranch and Ansible investment you made two months ago.
Today proves that's a yes, not a compromise."*

## 1. The asset graph — four business units, one lineage graph

1. Open the full lineage graph. Point out the four business-unit groups —
   `battery_materials`, `catalysis`, `recycling`, `specialty_materials` —
   each landing in `corporate_rollup`.
2. Say: *"This is Wauter's own words — 'a spiderweb that is only getting
   more tangled.' Four independently-owned data products, one place to see
   all of them."*
3. Click `catalysis/raw_ac_daily_feed`. Say: *"This is the AC feed named
   directly in the pain example — one full-time engineer spends about two
   hours a day manually checking logs across disconnected tools for this
   exact data product. Watch what replaces that in a minute."*

## 2. The money shot — the 3-job sequential chain, live

1. Open `specialty_materials/raw_specialty_materials_output`. Say: *"This
   is the first job in Anthony's own example — three sequential data
   projects, where today, if the first one runs long, the other two sit
   stale until the next scheduled window, with no automatic recovery."*
2. Materialize it. Watch `staging/stg_specialty_materials`,
   `marts/fct_cross_bu_consolidated_ledger`, and
   `marts/executive_reporting_extract` recompute automatically, in order,
   with no manual trigger.
3. Say: *"This is the exact chain you described on the August 5 call —
   today the second and third jobs sit stale until the next fixed window;
   here, they fire the moment the first one clears. That's declarative,
   event-based automation — not a cron job someone has to babysit."*

## 3. Asset checks — what stops the manual log-checking

1. Click `specialty_materials/raw_specialty_materials_output`'s metadata
   panel. Point at `raw_specialty_materials_output_arrival` — passing,
   **blocking**. Say: *"If this feed lands late or incomplete, the chain
   you just watched refuses to compute on top of it — no silent partial
   day propagating forward."*
2. Click `catalysis/raw_ac_daily_feed`'s metadata panel. Point at
   `raw_ac_daily_feed_quality` — passing, warning. Say: *"This is what
   stops the AC team's two-hours-a-day manual check from being necessary —
   it runs on every materialization, not once a day by hand."*
3. Click `marts/fct_battery_carbon_footprint_inputs`. Point at
   `fct_battery_carbon_footprint_inputs_completeness` — **blocking**. Say:
   *"This feeds the EU Battery Regulation carbon-footprint declaration —
   mandatory for your batteries above 2kWh starting this February. This
   check is what makes that declaration defensible: it never computes on
   an incomplete day."*

## 4. Coexistence — Stonebranch stays, Dagster observes

1. Open `adf_pipeline_stonebranch_nightly_mesh_sync` in the
   `legacy_orchestration` group. Say: *"Stonebranch keeps doing exactly
   what you bought it for two months ago — infrastructure scheduling. This
   is Dagster watching what it triggers, not replacing it."*
2. Point at the `legacy_system_boundary` metadata field. Say: *"This isn't
   a migration-in-progress boundary — it's the steady-state coexistence
   your business case argues for."*
3. Click through the lineage edge into `recycling/raw_recycling_batch_yield`.
   Say: *"This is the one point in the graph where the legacy-scheduled
   and Dagster-owned sides visibly meet — Stonebranch/ADF lands the raw
   extract, Dagster owns everything from here: freshness, quality,
   cross-BU lineage, all the way to the corporate rollup."*

## 5. Freshness and observation — how you'd know something broke

1. Open `marts/fct_cross_bu_consolidated_ledger`'s metadata panel. Point at
   the **freshness policy**. Say: *"This is how you'd know something broke
   without someone noticing a stale Power BI report first."*
2. Open `power_bi_mesh_health_dashboard_refresh`. Say: *"This is the
   dashboard where staleness is currently first noticed — often, per your
   own notes, by a business team reading a stale report. It's now a
   first-class asset in the same lineage graph, not a disconnected
   downstream surprise."*
3. Point at the Sensors tab —
   `databricks_workspace_observation_sensor` and
   `legacy_orchestration_observation_sensor`. Say: *"Both of these detect
   runs that happened outside Dagster entirely — a data scientist
   re-running a notebook by hand, or Stonebranch firing on its own. 'What
   happens when it wasn't Dagster that started it?' is answered here, not
   assumed away."*

## 6. What Dagster+ adds on top (don't build it live — point at it)

Say: *"Everything from here forward is not code I wrote for this demo —
it's the actual platform, and it's the direct answer to the objection in
your own business case: that Dagster has to be justified as a fourth tool,
not just against what it can do in isolation."*

1. **Native alerting** (Slack/Teams) on check and freshness-policy
   failures — the direct fix for "at least one team's workflow is down on
   any given day" with no central alert today.
2. **Restart-from-failure** — the direct fix for "root-causing a failure
   today means manually checking logs across Azure Data Factory,
   Python/PySpark jobs, and Databricks separately."
3. **Asset lineage visualization** — literally the untangled version of
   Wauter's spiderweb, one graph, four business units, one place to look.

## 7. Close

Say: *"Everything you saw today — the Databricks jobs, the ADF pipeline
observation, the dbt tests, the freshness policies, the lineage — works
identically the day this points at your real Databricks workspace, your
real Azure Data Factory, and your real Power BI workspace instead of local
DuckDB and simulated runs. Flipping each one on is a `demo_mode: false`
line plus real credentials — same asset graph, same checks, same YAML
either way."*

Then: *"The open question isn't whether this is technically capable — it's
whether coexistence with Stonebranch and Ansible actually holds up. What
would you need to see, beyond today, to bring this to Marten Zieris's
transformation roadmap conversation?"*

## What's out of scope today (say if asked, don't volunteer)

- **No change to Stonebranch/Ansible** — explicitly out of scope; this is
  coexistence, not a migration proposal.
- **No EU Battery Passport calculation logic** — Dagster's role is
  upstream data reliability feeding that process, not the carbon-footprint
  calculation itself.
- **No confirmed warehouse/lakehouse product** — be direct that DuckDB
  stands in for whatever Umicore's real Databricks/Synapse target turns
  out to be; the AE notes never confirm it.
- **No fifth business unit** — four named units only (Battery Materials
  Solutions, Catalysis, Recycling, Specialty Materials); don't speculate
  about the business case's illustrative "5 business units" ROI row.
- **No planted failure anywhere** — every check, freshness policy, and
  automation condition shown is real and currently green; the alerting and
  restart-from-failure story is told entirely through the Dagster+
  walkthrough and talk track above, never a staged break.
