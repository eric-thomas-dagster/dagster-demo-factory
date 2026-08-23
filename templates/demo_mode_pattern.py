"""Reference pattern: adding `demo_mode` to a real Dagster component.

Read this before writing any demo component. It encodes the one design rule
that makes these demos survive contact with a technical audience.

THE RULE
--------
Subclass the real component. Fake ONLY the outermost I/O call -- the single
method that crosses the network. Everything else runs the real code path.

Asset keys, asset specs, partition definitions, dependency edges, metadata,
check definitions, and the YAML schema MUST be identical whether `demo_mode`
is true or false.

WHY IT MATTERS
--------------
The demo's credibility rests on one moment: the prospect asks "does this
actually work against our Snowflake?" and we flip `demo_mode: false`, drop in
their credentials, and it runs. That moment only works if demo mode was never
a separate implementation.

The failure mode to avoid is a "demo component" that hand-rolls a plausible
asset graph with hardcoded data. It looks the same in a screenshot and is
worthless the moment anyone asks a real question -- and a data engineer in a
POC evaluation always asks a real question.

Concretely, this means:

  DON'T  write a new Component that emits similar-looking assets
  DON'T  branch inside build_defs to construct different asset specs
  DON'T  swap the partition definition, even to make the demo run faster
  DO     subclass and override the fetch/execute boundary only
  DO     let the real component build every Definitions object
"""

from __future__ import annotations

from typing import Any

import dagster as dg
import pandas as pd
from pydantic import Field

# The real component, installed from the community registry via:
#   uvx --from dagster-community-components-cli dagster-component add snowflake_query
from components.ingestion.snowflake_query import SnowflakeQueryComponent

from demo_data.generators import generate_frame_for_query


class DemoSnowflakeQueryComponent(SnowflakeQueryComponent):
    """`SnowflakeQueryComponent` that can serve synthetic rows instead of querying.

    Subclasses rather than replaces the real component, so the asset graph,
    partitions, metadata, and YAML schema are inherited unchanged. The only
    behavioural difference is where the DataFrame comes from.

    Every field below is additive -- an existing `defs.yaml` written against
    `SnowflakeQueryComponent` stays valid, and switching between demo and live
    is a one-line change to `demo_mode`.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Serve deterministic synthetic rows instead of querying Snowflake. "
            "Set false and supply real credentials to run against a live account."
        ),
    )
    demo_seed: int = Field(
        default=20260824,
        description=(
            "Seed for synthetic generation. Fixed so repeated demo runs produce "
            "identical numbers -- a demo whose row counts drift between runs "
            "invites questions we don't want to be answering live."
        ),
    )
    demo_row_count: int = Field(
        default=50_000,
        description=(
            "Rows per partition. Should be plausible for the prospect's actual "
            "volume; the number is visible in asset metadata."
        ),
    )
    demo_anomaly_partition: str | None = Field(
        default=None,
        description=(
            "Partition key to deliberately corrupt so a downstream asset check "
            "fails. This is the demo's money shot -- an all-green graph proves "
            "nothing about data quality tooling."
        ),
    )

    def _execute_query(
        self,
        context: dg.AssetExecutionContext,
        query: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Return query results, synthetically when `demo_mode` is set.

        This is the entire seam. `_execute_query` is the innermost method that
        actually opens a Snowflake connection; everything upstream of it --
        asset construction, partition mapping, metadata emission -- is the real
        component's code and runs identically in both modes.

        When overriding this pattern for a different component, find the
        equivalent single method and override only that one. If the component
        has no such seam, add one by extracting the network call into its own
        method and upstreaming that refactor to the registry rather than
        working around it here.
        """
        if not self.demo_mode:
            return super()._execute_query(context, query, **kwargs)

        partition_key = (
            context.partition_key if context.has_partition_key else None
        )

        frame = generate_frame_for_query(
            query=query,
            row_count=self.demo_row_count,
            seed=self.demo_seed,
            partition_key=partition_key,
        )

        if partition_key is not None and partition_key == self.demo_anomaly_partition:
            frame = _inject_anomaly(frame)
            context.log.info(
                "Demo mode: injected anomaly into partition %s so the downstream "
                "freshness/quality check has something to catch.",
                partition_key,
            )

        # Mirror the metadata the real path emits, so the UI looks identical.
        context.add_output_metadata(
            {
                "dagster/row_count": len(frame),
                "demo_mode": True,
                "source": dg.MetadataValue.text(
                    "synthetic -- set demo_mode: false in defs.yaml to query Snowflake"
                ),
            }
        )
        return frame


def _inject_anomaly(frame: pd.DataFrame) -> pd.DataFrame:
    """Corrupt a frame in a way the demo's asset checks are built to detect.

    Kept deliberately crude and obvious: nulls in a non-nullable business key
    plus a negative amount. The point is for the check failure to be legible
    when I click into it on a shared screen, not for the corruption to be subtle.
    """
    corrupted = frame.copy()
    n_null = max(1, len(corrupted) // 100)
    corrupted.loc[corrupted.index[:n_null], corrupted.columns[0]] = None
    if "amount" in corrupted.columns:
        corrupted.loc[corrupted.index[:n_null], "amount"] = -1.0
    return corrupted


# ---------------------------------------------------------------------------
# Corresponding defs.yaml -- note that flipping the demo off is one line.
#
#   type: demo_components.DemoSnowflakeQueryComponent
#   attributes:
#     demo_mode: true                     # <-- the only thing that changes
#     demo_seed: 20260824
#     demo_row_count: 50000
#     demo_anomaly_partition: "2026-08-19"
#     # --- everything below is the real component's schema, untouched ---
#     query: |
#       select claim_id, member_id, provider_npi, amount, adjudicated_at
#       from raw.claims
#       where adjudicated_at::date = '{partition_key}'
#     asset_key: ["raw", "claims_daily"]
#     partitions:
#       type: daily
#       start_date: "2026-06-01"
#     snowflake:
#       account: "{{ env.SNOWFLAKE_ACCOUNT }}"
#       warehouse: "{{ env.SNOWFLAKE_WAREHOUSE }}"
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Variant: components whose seam is a resource rather than a method.
#
# Some components take a resource (a client object) instead of doing I/O
# inline. For those, don't override build_defs -- override the resource the
# component resolves, so the component's own logic still drives everything.
#
#   class DemoSnowflakeResource(SnowflakeResource):
#       demo_mode: bool = True
#
#       def get_connection(self):
#           if not self.demo_mode:
#               return super().get_connection()
#           return _FakeConnection(seed=self.demo_seed)
#
# Then point the real component at the demo resource in defs.yaml. The
# component is completely unmodified -- which is the strongest version of
# this pattern, and the one to reach for first.
# ---------------------------------------------------------------------------
