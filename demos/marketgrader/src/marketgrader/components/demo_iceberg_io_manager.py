"""Demo-mode seam on top of the REAL registry Iceberg IO manager.

`IcebergIOManagerComponent` (imported below, unmodified) is the genuine
community-registry component installed via
`dagster-component add iceberg_io_manager` --
`src/marketgrader/components/iceberg_io_manager/` -- it wraps the official
`dagster-iceberg` package's `PyArrowIcebergIOManager`. This module does not
reimplement it; it adds exactly the resource-level seam
`templates/demo_mode_pattern.py` prescribes: `demo_mode` swaps the
`catalog_uri`/`warehouse` the real component's own fields already accept for
a local, zero-setup PyIceberg SQL (SQLite) catalog instead of a live
Iceberg/Polaris REST endpoint -- no MarketGrader lake credentials exist.
Every field the real component declares (`resource_key`, `catalog_name`,
`namespace`, `catalog_uri`, `warehouse`) stays on this subclass completely
unchanged, so the YAML schema is identical in both modes. `demo_mode: false`
calls `super().build_defs()` -- the exact, untouched registry component --
so pointing this at MarketGrader's real Polaris catalog is a one-line change
plus real `catalog_uri`/`warehouse` values, never a rewrite.

This is genuinely Iceberg -- real Iceberg table format, real Iceberg catalog
protocol (PyIceberg's `SqlCatalog`), real partition-aware writes -- not a
DuckDB file wearing an Iceberg badge. Verified end-to-end against a scratch
project before wiring into this one (two `DailyPartitionsDefinition`
partitions written and read back through the actual
`dagster.resolve_implicit_job_def_def_for_assets` /
`execute_in_process` path, dagster-iceberg==0.3.14 / pyiceberg==0.11.1).

One real component gap, found while verifying and worked around below (not
a `demo_mode`-only concern -- it would bite in production against a fresh
namespace too): `PyArrowIcebergIOManager.handle_output` calls
`catalog.list_namespaces(schema)`, which raises `NoSuchNamespaceError` if the
namespace doesn't already exist -- the component never creates it. Worked
around here by bootstrapping the namespace once in `build_defs` (both
`demo_mode` branches; see `_ensure_namespace`). Recorded in
`component-feedback/2026-09-24-iceberg-io-manager-namespace-bootstrap.md`.
"""

import dagster as dg
from pydantic import Field

from marketgrader.components.iceberg_io_manager.component import IcebergIOManagerComponent
from marketgrader.demo_data.lakehouse import demo_iceberg_catalog_properties


def _ensure_namespace(catalog_name: str, namespace: str, properties: dict[str, str]) -> None:
    """See gap #1 in the module docstring -- the real component never does
    this, in either mode, so a fresh warehouse (demo or real) fails on its
    very first write with `NoSuchNamespaceError` unless something bootstraps
    the namespace first."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(catalog_name, **properties)
    catalog.create_namespace_if_not_exists(namespace)


class DemoIcebergIOManagerComponent(IcebergIOManagerComponent):
    """`IcebergIOManagerComponent` that targets a local, zero-setup PyIceberg
    SQL catalog in demo mode. Set `demo_mode: false` and supply the real
    component's own `catalog_uri`/`warehouse` fields (unchanged) to run this
    exact component against MarketGrader's real Iceberg/Polaris lake.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Use a local, zero-setup PyIceberg SQL (SQLite) catalog instead "
            "of a live Iceberg/Polaris REST catalog -- no MarketGrader lake "
            "credentials exist. Set false and supply catalog_uri/warehouse "
            "(the real IcebergIOManagerComponent's own fields, unchanged) "
            "to point this at MarketGrader's real lake."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            _ensure_namespace(
                self.catalog_name,
                self.namespace,
                {k: v for k, v in {"uri": self.catalog_uri, "warehouse": self.warehouse}.items() if v},
            )
            return super().build_defs(context)

        from dagster_iceberg.config import IcebergCatalogConfig
        from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

        properties = demo_iceberg_catalog_properties()
        _ensure_namespace(self.catalog_name, self.namespace, properties)

        io_manager = PyArrowIcebergIOManager(
            name=self.catalog_name,
            config=IcebergCatalogConfig(properties=properties),
            namespace=self.namespace,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
