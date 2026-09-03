"""Asset-body factory for the warehouse-backed groups, writing real rows
through a real IO manager -- `demo_postgres_io_manager.py` or
`demo_snowflake_io_manager.py`, both subclasses of genuine community-registry
components (rungs 2/3 of the escalation ladder; see those modules'
docstrings). This class supplies no integration of its own -- it is not a
stand-in for Postgres or Snowflake, and it never appears in `kinds`. Its
only job is declaring, from a YAML list of `AssetSpec`s, the small
deterministic stub body every one of those specs needs so the configured
IO manager has something real to write.

Per the brief's graph-first Fidelity directive plus CLAUDE.md's Axis 2
("stubbed... trivial content, not real business calculation"), the rows
are a handful of deterministic, seeded placeholder values -- not a
synthetic-data generator modelling rental contracts or dealer sales. What
matters for Axis 1 is that they are written and read back through a real
IO manager, not that the numbers mean anything.

Same registry gap as the no-op predecessor this replaces
(`component-feedback/2026-08-28-graph-first-assets.md`, first written for
demos/detroit-dwsd): the registry has no component for "declare a list of
assets from YAML with a shared trivial body," because that's a generic
authoring convenience, not an integration domain -- rungs 1-3 don't apply
to it. What changed in this build is that the body now performs genuine
I/O via `io_manager_key` instead of only logging; the registry gap itself
is unchanged.

One instance covers every asset in a `defs.yaml` file -- adding the
prospect's next source table is one more `assets:` entry, never another
component instance or another Python file.
"""

import hashlib
from datetime import datetime

import dagster as dg
import pandas as pd
from pydantic import Field

DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone="UTC",
)

COMPANY_PARTITIONS = dg.StaticPartitionsDefinition(["noleggiare", "tomasi_auto"])

COMPANY_DATE_PARTITIONS = dg.MultiPartitionsDefinition(
    {"date": DAILY_PARTITIONS, "company": COMPANY_PARTITIONS}
)

STUB_ROW_COUNT = 5

# Every partitioned asset this component builds is on DAILY_PARTITIONS or
# COMPANY_DATE_PARTITIONS (the two shared template vars below), so the
# `partition_expr` column names the real IO manager needs to scope its
# per-partition DELETE+INSERT are fixed and known up front rather than
# per-asset config.
DATE_PARTITION_EXPR_COLUMN = "date"
COMPANY_PARTITION_EXPR_COLUMN = "company"


def _stub_dataframe(
    asset_key: str, date_value: datetime | None, company_value: str | None
) -> pd.DataFrame:
    """A handful of deterministic, seeded rows -- same asset key and
    partition values always produce the same rows, so repeated demo runs
    (and validate_e2e.py) never see row counts drift, per house rules.

    Includes a real `date` and/or `company` column, matching whichever
    partition dimensions the asset has, because the real IO manager's
    partition-scoped DELETE+INSERT filters on those columns directly --
    see `_partition_expr_metadata` below.
    """
    digest = hashlib.sha256(f"{asset_key}:{date_value}:{company_value}".encode()).hexdigest()
    seed = int(digest, 16) % 1_000_000
    data: dict[str, list] = {
        "id": [seed + i for i in range(STUB_ROW_COUNT)],
        "source_asset": [asset_key] * STUB_ROW_COUNT,
    }
    if date_value is not None:
        data[DATE_PARTITION_EXPR_COLUMN] = [date_value] * STUB_ROW_COUNT
    if company_value is not None:
        data[COMPANY_PARTITION_EXPR_COLUMN] = [company_value] * STUB_ROW_COUNT
    return pd.DataFrame(data)


def _partition_expr_metadata(partitions_def: dg.PartitionsDefinition | None) -> dict:
    """Tell the real IO manager which column each partition dimension lives
    in -- required for its partition-scoped DELETE+INSERT
    (`dagster._core.storage.db_io_manager`). Every partitions_def this
    component sees is one of the two shared template vars above, so the
    column names are fixed.
    """
    if partitions_def is None:
        return {}
    if isinstance(partitions_def, dg.MultiPartitionsDefinition):
        return {
            "partition_expr": {
                "date": DATE_PARTITION_EXPR_COLUMN,
                "company": COMPANY_PARTITION_EXPR_COLUMN,
            }
        }
    return {"partition_expr": DATE_PARTITION_EXPR_COLUMN}


class WarehouseTableAssetsComponent(dg.Component, dg.Resolvable, dg.Model):
    """Materializes each declared `AssetSpec` with a stub-data body that
    writes through a real IO manager.

    Asset keys, deps, partitions, metadata, and checks all come from the
    spec itself. `io_manager_key` points every asset this instance builds
    at one shared IO manager resource -- `postgres_io_manager` or
    `snowflake_io_manager`, registered in `defs/resources/defs.yaml` -- so
    the write is genuine, not simulated.
    """

    assets: list[dg.ResolvedAssetSpec]
    io_manager_key: str = Field(
        description=(
            "Resource key of the IO manager these assets write through, "
            "e.g. 'postgres_io_manager' or 'snowflake_io_manager' -- "
            "registered once in defs/resources/defs.yaml."
        )
    )

    @staticmethod
    @dg.template_var
    def daily_partitions() -> dg.DailyPartitionsDefinition:
        """Shared instance so cross-asset partition mappings resolve by identity."""
        return DAILY_PARTITIONS

    @staticmethod
    @dg.template_var
    def company_date_partitions() -> dg.MultiPartitionsDefinition:
        """Shared instance for the cross-company consolidated fact and its
        Snowflake-variant twin -- date x company, per the brief's explicit
        directive that company be a real partition dimension, not a tag.
        """
        return COMPANY_DATE_PARTITIONS

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[self._build_asset(spec) for spec in self.assets])

    def _build_asset(self, spec: dg.AssetSpec) -> dg.AssetsDefinition:
        op_name = "_".join(spec.key.path)
        spec_with_io = spec.with_io_manager_key(self.io_manager_key)
        partition_expr = _partition_expr_metadata(spec.partitions_def)
        if partition_expr:
            spec_with_io = spec_with_io.replace_attributes(
                metadata={**spec_with_io.metadata, **partition_expr}
            )
        asset_key_str = spec.key.to_user_string()

        @dg.multi_asset(specs=[spec_with_io], name=op_name)
        def _materialize(context: dg.AssetExecutionContext) -> pd.DataFrame:
            date_value: datetime | None = None
            company_value: str | None = None
            if context.has_partition_key:
                partition_key = context.partition_key
                if isinstance(partition_key, dg.MultiPartitionKey):
                    date_value = datetime.strptime(
                        partition_key.keys_by_dimension["date"], "%Y-%m-%d"
                    )
                    company_value = partition_key.keys_by_dimension["company"]
                else:
                    date_value = datetime.strptime(partition_key, "%Y-%m-%d")

            frame = _stub_dataframe(asset_key_str, date_value, company_value)
            context.add_output_metadata(
                {
                    "stub_row_count": len(frame),
                    "source": dg.MetadataValue.text(
                        "stubbed synthetic rows -- graph-first fidelity per the "
                        "brief, written through a real IO manager rather than a "
                        "no-op body"
                    ),
                }
            )
            return frame

        return _materialize
