# Fabric trigger-and-observe as a named, partitioned, checked asset

**Prospect:** Stellantis Financial Services (orchestrate-existing-workloads demo).

## What was needed

A component that produces one named Dagster asset per Fabric pipeline,
supporting our own asset key, upstream deps, a `MultiPartitionsDefinition`
on one feed, freshness policies, retry policies, and asset checks wired to
specific assets -- while triggering-and-polling the pipeline via the Fabric
REST API (demo-mode-fakeable) rather than recomputing the transformation
itself. Needed 17 of these across bronze/silver/gold/reporting.

## What was searched

`dagster-component search` (single-string queries; multi-arg positional
queries are rejected by the current CLI -- see LEARNINGS.md correction):
`"microsoft fabric"`, `"fabric pipeline trigger"`, `"fabric lakehouse"`,
`"onelake"`, `"fabric workspace"`, `"power bi refresh"`, `"powerbi"`,
`"power bi"`.

## What came closest

- **`fabric_workspace`** (category `integration`, produces `multi_asset`) --
  imports whatever items already exist in a *live* Fabric workspace as
  Dagster assets. Doesn't fit: it discovers assets from a real connection at
  load time, so it can't produce a named asset ahead of one, has no
  partitions support (`x-dagster-io.supports_partitions: false` on the
  sibling resource component, and no partition config surfaced on this one
  either), and its asset keys come from the workspace's own item names, not
  ours.
- **`fabric_pipeline_trigger_job`** (category `jobs`, produces `job` +
  `schedule`) -- triggers one Fabric pipeline/notebook/dataflow on a
  schedule. Doesn't fit: it's a bare op-job, not a lineage-graph asset, so it
  can't carry an `AssetKey`, deps, a partitions_def, a freshness policy, or
  have an `@asset_check` attached to it.
- **`dataframe_to_fabric_lakehouse`** (category `sink`, produces `asset`) --
  closest in shape (it is an asset), but it's a sink: it still needs a
  Python-supplied DataFrame from somewhere upstream, and its schema has no
  partitions/checks/freshness surface either -- using it would mean wrapping
  it in the same custom logic anyway, with an extra indirection.
- **`fabric_workspace_resource`** (category `resource`) -- not itself an
  asset-producing component, but the right *building block*: a thin,
  demo-mode-able REST client (`list_items` / `trigger_item_run` /
  `wait_for_run`). This is what the custom component below wraps.

## What was built instead

`FabricPipelineAssetComponent`
(`demos/stellantis-financial-services/src/stellantis_financial_services/components/fabric_pipeline_asset.py`),
a `dg.Component` that takes an asset key, generator key, deps (including one
`MultiToSingleDimensionPartitionMapping` dep), partitions shape
(`daily` | `multi_date_dealer_group`), freshness/automation/retry config,
and a `fabric_item_name` to trigger in real mode. It injects the
`FabricWorkspaceResource` from `fabric_workspace_resource` (subclassed with a
`demo_mode` seam per `templates/demo_mode_pattern.py`) and calls
`trigger_item_run` / `wait_for_run` in real mode, or a deterministic
generator in demo mode. One class, instantiated 17 times via `defs.yaml`
(see `defs/bronze/`, `defs/silver/`, `defs/gold/`, `defs/reporting/`).

A second small custom component, `DuckDbAssetCheckComponent`
(`components/duckdb_asset_check.py`), fills the analogous gap for asset
checks: no registry component expresses an arbitrary blocking/warning SQL
assertion against an asset materialized outside dbt. Instantiated 4 times.

## Suggested change

Give `fabric_pipeline_trigger_job` (or a new sibling component) an asset
mode: accept an `asset_key`, `deps`, and `partitions_def`-shaped config
(reusing whatever schema `fabric_workspace_resource`'s planned partition
support would use), and change its `build_defs` to return a
`dg.AssetsDefinition` wrapping the same trigger/poll body instead of a bare
`job`. That single change would have covered the bronze layer directly
(5 of the 17 instantiations here) without a custom component -- the deps/
freshness/checks would still need to compose from outside it, which today's
`AssetSpec`-based components already handle fine.
