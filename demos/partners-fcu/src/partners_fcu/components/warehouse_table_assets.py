"""Asset-body factory for the SQL-Server-badged ingestion layer, writing real
rows through `demo_mssql_io_manager.py` -- a subclass of the genuine
community-registry `mssql_io_manager` component (rung 2 of the escalation
ladder; see that module's docstring). This class supplies no integration of
its own -- it is not a stand-in for SQL Server, and it never appears in
`kinds`. Its only job is declaring, from a YAML list of `AssetSpec`s, the
small deterministic stub body every one of those specs needs so the
configured IO manager has something real to write.

Per the brief's graph-first Fidelity directive plus CLAUDE.md's Axis 2
("stubbed... trivial content, not real business calculation"), the rows are
a handful of deterministic, seeded placeholder values -- not a full
synthetic-data-generation apparatus. What matters for Axis 1 is that they
are written and read back through a real IO manager (so the blocking
completeness check and the downstream dbt source have real rows to query),
not that the numbers mean anything.

Same registry gap first recorded for demos/detroit-dwsd
(`component-feedback/2026-08-28-graph-first-assets.md`) and reused verbatim
for demos/noleggiare with real I/O: the registry has no component for
"declare a list of assets from YAML with a shared trivial body writing
through a configured IO manager," because that's a generic authoring
convenience, not an integration domain -- rungs 1-3 don't apply to it.

One instance covers every asset in `defs/ingestion/defs.yaml` -- adding
Partners FCU's next SQL Server source table is one more `assets:` entry,
never another component instance or another Python file.
"""

import hashlib
from datetime import datetime

import dagster as dg
import pandas as pd
from pydantic import Field

# Column the real IO manager's demo-mode DuckDB backing scopes its
# per-partition DELETE+INSERT on -- shared across all three raw tables so
# one component instance can pass the same `partition_expr` to every spec.
# See demo_mssql_io_manager.py.
DATE_PARTITION_EXPR_COLUMN = "partition_date"

# ~165,000 members -- member IDs are deterministic-per-row within a table,
# not globally unique across tables, since each raw table is its own
# simulated source extract.
_MEMBER_ID_SPACE = 165_000


def _stub_dataframe(asset_key: str, date_value: datetime, row_count: int) -> pd.DataFrame:
    """A handful of deterministic, plausible rows per raw table -- same
    asset key and partition value always produce the same rows, so
    repeated demo runs (and validate_e2e.py) never see counts drift, per
    house rules. Columns are domain-specific per table so the dbt staging
    layer downstream has real typed columns to select, not a placeholder
    blob.
    """
    digest = hashlib.sha256(f"{asset_key}:{date_value}".encode()).hexdigest()
    seed = int(digest, 16) % 1_000_000
    member_ids = [(seed + i * 7919) % _MEMBER_ID_SPACE for i in range(row_count)]

    if asset_key == "raw_member_transactions":
        types = ["deposit", "withdrawal", "transfer", "fee"]
        data = {
            "transaction_id": [seed + i for i in range(row_count)],
            "member_id": member_ids,
            "transaction_type": [types[i % len(types)] for i in range(row_count)],
            "amount": [round(10 + ((seed + i * 31) % 250000) / 100, 2) for i in range(row_count)],
        }
    elif asset_key == "raw_deposit_accounts":
        types = ["share_savings", "checking", "money_market", "certificate"]
        data = {
            "account_id": [seed + i for i in range(row_count)],
            "member_id": member_ids,
            "account_type": [types[i % len(types)] for i in range(row_count)],
            "balance": [round(100 + ((seed + i * 53) % 5_000_000) / 100, 2) for i in range(row_count)],
        }
    elif asset_key == "raw_loan_originations":
        types = ["auto", "personal", "mortgage", "heloc"]
        data = {
            "loan_id": [seed + i for i in range(row_count)],
            "member_id": member_ids,
            "loan_type": [types[i % len(types)] for i in range(row_count)],
            "principal_amount": [round(500 + ((seed + i * 97) % 4_000_000) / 100, 2) for i in range(row_count)],
        }
    else:
        raise ValueError(f"no stub schema defined for {asset_key!r}")

    data[DATE_PARTITION_EXPR_COLUMN] = [date_value] * row_count
    return pd.DataFrame(data)


class WarehouseTableAssetsComponent(dg.Component, dg.Resolvable, dg.Model):
    """Materializes each declared `AssetSpec` with a stub-data body that
    writes through a real IO manager (`mssql_io_manager`, registered in
    `defs/resources/defs.yaml`).

    Asset keys, deps, partitions, metadata, and checks all come from the
    spec itself; `row_count` is the only field this component adds, so a
    prospect's real per-table volume assumption is visible and editable
    per entry in the YAML, not buried in Python.
    """

    assets: list[dg.ResolvedAssetSpec]
    io_manager_key: str = Field(
        description=(
            "Resource key of the IO manager these assets write through -- "
            "'mssql_io_manager', registered once in defs/resources/defs.yaml."
        )
    )
    row_count: int = Field(
        default=100,
        description=(
            "Deterministic stub row count per partition, used when an asset's "
            "own spec metadata doesn't set 'demo_row_count'."
        ),
    )

    @staticmethod
    @dg.template_var
    def daily_partitions() -> dg.DailyPartitionsDefinition:
        from partners_fcu.components.partitions import DAILY_PARTITIONS_DEF

        return DAILY_PARTITIONS_DEF

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[self._build_asset(spec) for spec in self.assets])

    def _build_asset(self, spec: dg.AssetSpec) -> dg.AssetsDefinition:
        op_name = "_".join(spec.key.path)
        row_count = int(spec.metadata.get("demo_row_count", self.row_count))
        spec_with_io = spec.with_io_manager_key(self.io_manager_key)
        spec_with_io = spec_with_io.replace_attributes(
            metadata={**spec_with_io.metadata, "partition_expr": DATE_PARTITION_EXPR_COLUMN}
        )
        asset_key_str = spec.key.to_user_string()

        @dg.multi_asset(specs=[spec_with_io], name=op_name)
        def _materialize(context: dg.AssetExecutionContext) -> pd.DataFrame:
            date_value = datetime.strptime(context.partition_key, "%Y-%m-%d")
            frame = _stub_dataframe(asset_key_str, date_value, row_count)
            context.add_output_metadata(
                {
                    "dagster/row_count": len(frame),
                    "source": dg.MetadataValue.text(
                        "stubbed synthetic rows -- graph-first fidelity per the "
                        "brief, written through a real IO manager rather than a "
                        "no-op body"
                    ),
                }
            )
            return frame

        return _materialize
