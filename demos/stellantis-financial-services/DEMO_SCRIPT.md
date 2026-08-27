# DEMO_SCRIPT -- "The 700th Package"

Run-of-show for Stellantis Financial Services (Nick Gogos, Chris Rodriguez).
Give this live locally with `dg dev` -- Dagster+ Serverless storage is
ephemeral, so treat the deployed code location as proof the project is real,
not as the place to run this sequence (see README's "Local vs. Dagster+").

Start with `dg dev` open to http://localhost:3000, Assets → all assets,
grouped view. Before the room arrives, open Sensors and turn on both
`legacy_scheduler_observer` and `fabric_pipelines_external_run_observer` (or
click "Test sensor" on each once) so step 5 has observation history to show
rather than an empty timeline.

## 1. Orient on the graph (60s)

Point at the full graph: bronze (vendor-file ingestion) → silver
(conforming) → gold (marts) → reporting. Two assets in bronze --
`raw_dealer_floorplan_feed` and `raw_credit_bureau_pull` -- carry a visibly
different kind badge (`azure` alone) from everything else (`fabric` +
`azure`).

**Say:** "This is your vendor-file-to-ABS-pool flow -- loan and lease
originations, payments, dealer floorplan, credit bureau, all the way through
to the ABS pool eligibility number your capital markets team needs. Most of
this is already running on Fabric, or is being migrated right now. But two
of these feeds -- right here -- are still running exactly as they do today,
on your own SSIS packages, under your own scheduler. Both halves are in this
one graph, at the same time, because that's actually where you are."

## 2. THE MONEY SHOT -- the boundary crossing (2 min, don't rush this one)

Click `dim_dealer`. Point at its one upstream dependency,
`raw_dealer_floorplan_feed`. Open `raw_dealer_floorplan_feed`'s
materialization history / event log.

**Say:** "`dim_dealer` is 100% built and running on Fabric -- Dagster
triggers it, tracks it, gates it. But look at what feeds it: every event in
this asset's history is an `AssetObservation`, never a Dagster-triggered run.
That's because there's no Fabric pipeline behind this feed at all. Your own
scheduler runs this SSIS package today, and Dagster never touches it -- it
only learns, the moment your scheduler finishes, that the data landed. Both
are already in the same graph, right now, with the same freshness tracking,
not after the migration finishes."

Repeat briefly for `dim_borrower` → `raw_credit_bureau_pull` (a second,
independent boundary crossing -- credit bureau, not dealer floorplan).

**Say:** "This is the actual answer to 'can Dagster sit across a migration
that isn't finished' -- not a label in a metadata field, a real second
system feeding a real Dagster-triggered asset."

## 3. One component, one mapping table (60s)

Open `src/stellantis_financial_services/defs/fabric_pipelines/defs.yaml`.
Scroll to `assets_by_item_name`.

**Say:** "Every Fabric-migrated asset -- 15 of them -- comes from one
component instance and this mapping table. Your 30th migrated package, or
your 700th, is one more entry here. Not a new Python class, not a new
component instance. And when one of the two feeds you just saw finally cuts
over, it gains a second entry here -- nothing about its identity in the graph
changes, it just gains a Dagster-triggered execution path alongside the
observation it already has."

## 4. Everything's green, checks included (90s)

Click into `abs_pool_eligibility`. Show the Checks tab: green.

**Say:** "This is your money-shot asset -- it feeds directly into your 2026
ABS securitization calendar. This blocking check reconciles eligible balance
against total balance minus delinquent balance, every partition, every run.
You don't have to watch it fail to believe it works -- the check is right
here, computing a real condition from real data, and it gates this asset
before anything downstream computes on a bad number."

Click into `raw_loan_originations`, show its blocking completeness check.

**Say:** "Same story upstream -- a loan record missing its ID or dealer or
amount structurally cannot reach staging."

Click into `raw_dealer_floorplan_feed`'s Checks tab -- the lateness warning
check, also green.

**Say:** "And this check runs even though we never triggered this asset --
the same sensor that tells us your scheduler finished also evaluates whether
it finished on time. You get lateness visibility on a feed you haven't
migrated yet."

## 5. Freshness (30s)

Click `fact_delinquency_snapshot`, point at the Freshness tab (6h fail / 3h
warn).

**Say:** "This is your SLA tracking and lateness visibility -- declared once
on the asset, no custom dashboard. It pages someone when your delinquency
numbers go stale, whether the upstream data came from Fabric or from the
system you're leaving."

## 6. Targeted rematerialization (60s)

In the UI, select just `raw_loan_originations`, today's partition.
Materialize.

**Say, while it runs (seconds):** "Watch this -- I'm rematerializing one
day's originations. This is what replay and backfill look like day to day:
targeted, fast. Whether you're backfilling a schema change, reprocessing
after an upstream fix, or just an operator's judgment call -- same mechanism,
every time. And once it lands, everything downstream that depends on it
recomputes on its own -- that's `AutomationCondition.eager()`, not a
schedule you have to remember to fire."

## 7. Point at Dagster+ (30s -- can be screenshots if not live)

**Say:** "Restart-from-failure, re-run from point of failure, the backfill
UI, run history and duration trends, asset health -- all of this is native
Dagster+, not something we built for this demo. And your Teams channel gets
native alert policies on run failures, check failures, and freshness
violations -- no custom sensor code for your team to own and maintain."

## 8. Close (30s)

**Say:** "Half your portfolio's data is still coming from the system you're
leaving today, and it's already in the same lineage graph, with the same
freshness tracking, as the half you've moved. You don't have to finish this
migration to get value from Dagster, and you don't have to route everything
through us on day one. Your 700th package -- or your 30th under Dagster --
is one more row in a table, and until it's added, that package's data still
shows up right here, from the system it's actually running on."

---

**If asked "why doesn't anything break on screen":** "We build these
capabilities to show you the mechanism, not to fake a fire. The checks and
freshness policies you just saw are exactly what catches this in production
-- we'd rather show you they're real and correctly wired than stage
something breaking and recovering, which teaches you less about how this
actually behaves under a real failure."

**If asked "which packages are actually still on SSIS today":** "We picked
dealer floorplan and credit bureau as the representative example -- both are
third-party-integration-heavy feeds, a plausible last-to-migrate pair, but
that's our assumption for the demo, not something your AE confirmed with
you. The real question is whether the pattern generalizes to whichever
packages you actually have left, and it does -- it's the same external-asset
plus observation-sensor shape regardless of which two (or 683) packages
haven't cut over yet."
