# IcebergIOManagerComponent — namespace bootstrap gap for demos/marketgrader

**What was needed:** Persist `raw_price_feed` / `raw_corporate_actions` as
real Iceberg tables on a target Iceberg/Polaris lake, with a demo-mode seam
(no MarketGrader lake credentials exist) that's still genuinely Iceberg —
not a DuckDB file wearing an `iceberg` kind badge.

**What was searched** (all run with `--json`, from `demos/marketgrader`):

```
uvx --from dagster-community-components-cli dagster-component search "iceberg" --json
uvx --from dagster-community-components-cli dagster-component search "polaris catalog" --json
uvx --from dagster-community-components-cli dagster-component search "pyiceberg" --json
```

Results included `iceberg_io_manager` ("Wrap the official `dagster-iceberg`
IcebergPyarrowIOManager so assets persist as Iceberg tables"),
`iceberg_catalog_resource`, `iceberg_ingestion`, `external_iceberg_table`,
and `dataframe_to_iceberg_table`. `iceberg_io_manager` was the right fit —
an IO manager, not a discover-and-observe workspace component, since
MarketGrader's target lake is a write destination for new assets, not an
external system to trigger/observe.

**What came closest:** `iceberg_io_manager` itself — no rejection, used
as-is at rung 2/3 (subclassed only for the demo-mode catalog config and the
gap below).

**Verification before use:** because this is a write-path integration (not
observe-only, unlike the SSIS side), it was verified end-to-end in a scratch
project before being wired into the actual build — installed
`dagster-iceberg`/`pyiceberg`/`pyarrow`, constructed a
`PyArrowIcebergIOManager` against a local `sqlite:///` PyIceberg `SqlCatalog`
with a `file://` warehouse, and ran two `DailyPartitionsDefinition`
partitions through `Definitions.resolve_implicit_job_def_def_for_assets(...).execute_in_process(...)`,
confirming a real, typed (`pa.date32()`), partition-aware write and read-back
(dagster-iceberg==0.3.14, pyiceberg==0.11.1). One property of the underlying
`pyiceberg.catalog.load_catalog` worth noting for future builds: it infers
the `SqlCatalog` implementation from a `sqlite://` URI scheme even with no
explicit `"type": "sql"` key in `properties` — so the component's own
`IcebergCatalogConfig(properties={"uri": ..., "warehouse": ...})` (no `type`
field at all) works unmodified for a local catalog; this is **not** a gap.

**What was built instead:** `DemoIcebergIOManagerComponent`
(`demos/marketgrader/src/marketgrader/components/demo_iceberg_io_manager.py`),
a subclass overriding `build_defs` to swap in the local
`catalog_uri`/`warehouse` properties in demo mode, unchanged in real mode.

**Gap found (real for both modes, not demo_mode-specific):**
`PyArrowIcebergIOManager.handle_output` → `IcebergDbClient.ensure_schema_exists`
calls `catalog.list_namespaces(schema)`, which raises
`pyiceberg.exceptions.NoSuchNamespaceError` if the namespace doesn't already
exist — the component never creates it. A fresh warehouse (demo *or* real —
this would identically bite a first deploy against a brand-new Polaris
namespace) fails on its very first write with no actionable message pointing
at the fix. Worked around in `DemoIcebergIOManagerComponent.build_defs` by
calling `catalog.create_namespace_if_not_exists(self.namespace)` once before
constructing the IO manager, in **both** the demo and real-mode branches
(this fix isn't demo-mode-specific, so it isn't gated behind `demo_mode`).

**Suggested change** (smallest edit that would have closed the gap):
`IcebergIOManagerComponent.build_defs` should call
`catalog.create_namespace_if_not_exists(self.namespace)` once when
constructing the `PyArrowIcebergIOManager`, the same bootstrap step this
build now does externally. One `pyiceberg.catalog.load_catalog(...)` call
plus one `create_namespace_if_not_exists(...)` call, using the same
`catalog_name`/`catalog_uri`/`warehouse` properties the component already
builds — no new config surface needed.
