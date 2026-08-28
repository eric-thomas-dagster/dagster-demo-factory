# Graph-first, pass-bodied asset declarations from YAML

## What was needed

A way to declare a list of assets -- key, deps, group_name, kinds, owners,
metadata, partitions_def, automation_condition, freshness_policy -- entirely
from `defs.yaml`, where the materialization body is a no-op. This is the
"graph-first" fidelity mode (CLAUDE.md): the demo's story is lineage, checks,
freshness, and automation, not real data, so there's nothing for a real
integration component to fetch or write.

## What was searched

Run against `dagster-community-components-cli` 0.8.15, all with `--json`,
2026-08-28:

- `dagster-component search "generic asset spec yaml" --json` -> only
  `openapi_asset` (wrong domain: builds assets from a live OpenAPI spec)
- `dagster-component search "generic multi asset factory" --json` -> `[]`
- `dagster-component search "declarative asset yaml no-op" --json` -> `[]`
- `dagster-component search "external asset lineage placeholder" --json` ->
  only `agentic_pipeline` (wrong domain: LLM pipeline steps)

## What came closest

`openapi_asset` -- closest by keyword overlap, but it's an integration
component that builds assets from a live OpenAPI spec at state-write time.
No component in the registry addresses "declare N assets with real specs but
zero compute" -- that's a generic authoring primitive, not an integration
domain, so rungs 1-3 of the escalation ladder (native / registry-as-is /
registry-subclassed) don't apply; there's nothing to subclass.

## What was built instead

`GraphFirstAssetsComponent`
(`demos/detroit-dwsd/src/detroit_dwsd/components/graph_first_assets.py`) --
takes `assets: list[dg.ResolvedAssetSpec]` and wraps each spec in a
single-spec `@dg.multi_asset` with a body that only logs. Every other
attribute (partitions, metadata, checks, automation, freshness) is carried
entirely by the spec itself, resolved by Dagster's built-in
`dg.ResolvedAssetSpec` type -- the component contributes only the empty
execution function. One instance covers a whole source domain (3 instances,
11 assets total, in `defs/dwsd_ingestion`, `defs/dwsd_warehouse`,
`defs/dwsd_reporting`), so adding an asset is one more `assets:` entry.

## Suggested change

A small `graph_first_assets` (or `noop_assets`) component in the registry's
`infrastructure` category, with exactly this shape, would remove this
26-line file from every graph-first demo build going forward. It's
generically useful beyond this repo: any project that wants to sketch an
asset graph's lineage/metadata/checks before wiring real I/O would reach for
it.
