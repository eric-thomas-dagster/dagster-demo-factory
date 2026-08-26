# DEMO_SCRIPT -- "The 700th Package"

Run-of-show for Stellantis Financial Services (Nick Gogos, Chris Rodriguez).
Give this live locally with `dg dev` -- Dagster+ Serverless storage is
ephemeral, so treat the deployed code location as proof the project is real,
not as the place to run this sequence (see README's "Local vs. Dagster+").

Start with `dg dev` open to http://localhost:3000, Assets → all assets,
grouped view.

## 1. Orient on the graph (60s)

Point at the full graph: bronze (vendor-file ingestion) → silver
(conforming) → gold (marts) → reporting, with `fabric` / `azure` / `powerbi`
kind badges throughout.

**Say:** "This is your vendor-file-to-ABS-pool flow -- loan and lease
originations, payments, dealer floorplan, credit bureau, all the way through
to the ABS pool eligibility number your capital markets team needs. Every
one of these is a Fabric pipeline that already exists, or is being migrated
right now. Dagster isn't recomputing this logic in a new engine -- it's
triggering, tracking, and gating the pipelines you're already building."

## 2. One component, one mapping table (60s)

Open `src/stellantis_financial_services/defs/fabric_pipelines/defs.yaml`.
Scroll to `assets_by_item_name`.

**Say:** "This whole graph -- all 17 assets -- comes from one component
instance and this mapping table. Your 30th migrated package, or your 700th,
is one more entry here. Not a new Python class, not a new component
instance. That's the scaling story."

## 3. Everything's green, checks included (90s)

Click into `abs_pool_eligibility`. Show the Checks tab: both checks green.

**Say:** "This is your money-shot asset -- it feeds directly into your 2026
ABS securitization calendar. This blocking check reconciles eligible balance
against total balance minus delinquent balance, every partition, every run.
You don't have to watch it fail to believe it works -- the check is right
here, computing a real condition from real data, and it gates this asset
before anything downstream computes on a bad number."

Click into `raw_loan_originations`, show its blocking completeness check.

**Say:** "Same story upstream -- a loan record missing its ID or dealer or
amount structurally cannot reach staging. That's your 'failure recovery is
manual' problem solved structurally, not by hoping someone notices."

## 4. Freshness (30s)

Click `fact_delinquency_snapshot`, point at the Freshness tab (6h fail / 3h
warn).

**Say:** "This is your SLA tracking and lateness visibility -- declared once
on the asset, no custom dashboard. This is what pages someone when your
delinquency numbers go stale."

## 5. THE MONEY SHOT -- targeted rematerialization (90s)

In the UI, select just `raw_dealer_floorplan_feed`, partition `south` /
today. Materialize.

**Say, while it runs (seconds):** "Watch this -- I'm rematerializing one
region's one day. Not the other three regions, not the other 699 packages'
worth of pipeline behind this graph. This is what replay and backfill look
like day to day: targeted, fast, and it doesn't touch anything it doesn't
need to. Whether you're backfilling a schema change, reprocessing after an
upstream fix, or just an operator's judgment call -- same mechanism, every
time."

Show it complete, then click `dim_dealer` and point out it's ready to
recompute (declarative automation would pick it up automatically outside
this manual demo click).

**Say:** "And once corrected data lands, everything downstream that depends
on it recomputes on its own -- that's `AutomationCondition.eager()`, not a
schedule you have to remember to fire."

## 6. THE COEXISTENCE MOMENT (60s) -- don't skip this one

Navigate to `raw_credit_bureau_pull` or `raw_dealer_floorplan_feed` (south
partition). Point at the latest event in its timeline -- an `AssetObservation`
with `fabric/triggered_by` metadata reading "SFS homegrown scheduler" or
"Operator ran the Fabric pipeline by hand mid-migration."

If the sensor hasn't ticked yet, open Sensors → `fabric_pipelines_external_run_observer`
and click "Test sensor" (or wait for its 30s interval) before this step.

**Say:** "This is the answer to the question you're actually evaluating --
what happens when it wasn't Dagster that started it? Your own scheduler is
still going to trigger some of these 700 packages during the migration. When
it does, it shows up right here, in the same lineage graph, with the same
freshness tracking, as anything Dagster itself triggered. You are not being
asked to route everything through us on day one."

## 7. Point at Dagster+ (30s -- can be screenshots if not live)

**Say:** "Restart-from-failure, re-run from point of failure, the backfill
UI, run history and duration trends, asset health -- all of this is native
Dagster+, not something we built for this demo. And your Teams channel gets
native alert policies on run failures, check failures, and freshness
violations -- no custom sensor code for your team to own and maintain."

## 8. Close (30s)

**Say:** "Dagster sits on top of what you're building in Fabric today. It
gives you the visibility your homegrown layer can't, and it doesn't ask you
to rebuild anything or cut over all at once. Your 700th package -- or your
30th under Dagster -- is one more row in a table."

---

**If asked "why doesn't anything break on screen":** "We build these
capabilities to show you the mechanism, not to fake a fire. The checks and
freshness policies you just saw are exactly what catches this in production
-- we'd rather show you they're real and correctly wired than stage
something breaking and recovering, which teaches you less about how this
actually behaves under a real failure."
