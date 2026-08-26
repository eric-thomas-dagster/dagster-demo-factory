# `fabric_workspace`'s `polling_sensor` field is declared but never built

**Prospect:** Stellantis Financial Services ("The 700th Package").

This is not a rung-4 report -- the build stayed at rung 3 (subclassed
`fabric_workspace`, one instance, an explicit `assets_by_item_name` mapping
table). It's filed anyway because it's a concrete, verified bug in the
registry component that the CLAUDE.md escalation-ladder table doesn't have a
slot for: not "doesn't fit," but "the documented config surface doesn't do
what it says."

## What was needed

The brief's central ask for this demo (a homegrown-scheduler-to-Fabric
migration, "coexistence" pattern) is: a package still triggered outside
Dagster during the migration should show up in the same lineage graph with
an `AssetObservation`. `fabric_workspace`'s README and field docstring both
document `polling_sensor` (alias `generate_sensor`) as exactly this:

> "If true, adds a polling sensor that detects new Fabric item job
> completions and emits AssetObservation events. [...] Off by default --
> opt in explicitly."

## What was searched

This isn't a "nothing fits" gap -- the component's own docs said the feature
existed. Verified by reading source, not by searching:

- `integrations/fabric_workspace/component.py` (fetched via
  `raw.githubusercontent.com/.../integrations/fabric_workspace/component.py`,
  680 lines, read in full): `polling_sensor` is declared as a `Field` on
  `FabricWorkspaceComponent` (with the `generate_sensor` alias) and appears
  nowhere else in the file -- not read, not branched on, no
  `@dg.sensor`-decorated function anywhere in the module.
- `dagster.components.component.state_backed_component.StateBackedComponent`
  (installed package source, `.venv/.../state_backed_component.py`, read in
  full): `build_defs` calls exactly one thing --
  `self.build_defs_from_state(context, state_path=state_path)` -- and
  returns its result directly. There is no sensor-assembly step anywhere in
  the base class either.

So `generate_sensor: true` in a `defs.yaml` silently changes nothing. This
is the exact gap that sank three prior builds of this demo (see this repo's
`state/ledger.json` entry for `stellantis-financial-services`, and the
brief's own rewrite history) -- each one set the flag, confirmed it in the
diff, and shipped a demo that only ever showed Dagster-triggered runs.

## What was built instead

`DemoFabricWorkspaceComponent.build_defs_from_state`
(`demos/stellantis-financial-services/src/stellantis_financial_services/components/fabric_pipelines_demo.py`)
constructs the sensor itself when `self.polling_sensor` is set, gated on the
same field the base component already declares -- no new config surface
added, just the missing wiring. In demo mode it rotates through a fixed,
deterministic list of "externally triggered" items and emits
`AssetObservation` events via `SensorResult(asset_events=[...])`; in real
mode it's stubbed to poll the same Fabric run-history endpoint
`_trigger_item_run` already talks to, filtered to runs the component didn't
itself trigger.

## Suggested change

In `FabricWorkspaceComponent.build_defs_from_state`, when
`self.polling_sensor` is true, append a `@dg.sensor`-decorated
`SensorDefinition` to the returned `Definitions` that polls
`GET /workspaces/{id}/items/{item_id}/jobs/instances` (or the workspace-level
job-history equivalent) per imported runnable item, tracks a `since`
cursor, and emits `AssetObservation` for any job instance whose ID hasn't
been seen -- mirroring whatever `polling_sensor` already does on
`FivetranAccountComponent` / `SnowflakeWorkspaceComponent`, which the field's
own docstring cites as the pattern being matched. That single fix closes the
gap for every consumer of this component, not just this demo.
