"""Reference pattern: adding `demo_mode` to a real Dagster component.

Read this before writing any demo component. It encodes the two design rules
that make these demos survive contact with a technical audience.

RULE 1 -- SUBCLASS, DON'T REIMPLEMENT
--------------------------------------
Subclass the real component. Fake ONLY the outermost I/O call -- the single
method that crosses the network. Everything else runs the real code path.

Asset keys, asset specs, partition definitions, dependency edges, metadata,
check definitions, and the YAML schema MUST be identical whether `demo_mode`
is true or false.

The demo's credibility rests on one moment: the prospect asks "does this
actually work against our Snowflake?" and we flip `demo_mode: false`, drop in
their credentials, and it runs. That only works if demo mode was never a
separate implementation.

  DON'T  write a new Component that emits similar-looking assets
  DON'T  branch inside build_defs to construct different asset specs
  DON'T  swap the partition definition, even to make the demo run faster
  DO     subclass and override the fetch/execute boundary only
  DO     let the real component build every Definitions object

RULE 2 -- ASSETS ARE IDEMPOTENT; THE SOURCE CHANGES, NOT THE ASSET
-------------------------------------------------------------------
Recovery is never an action inside Dagster. There is no heal step, no reset
asset, no repair job. Rematerializing a partition re-reads the source and picks
up whatever is there now -- exactly as it would in production.

So a late-arriving feed is modelled as SOURCE STATE, not as a demo toggle:
the carrier's API has no rows for that partition at 2pm, and has them at 4pm.
The asset is unchanged. Its input changed.

  DON'T  create a `healed_partitions` or `demo_control` asset
  DON'T  create a heal/reset job (op jobs are for real side-effectful work
         a prospect would recognise, like shipping logs -- not demo state)
  DON'T  require a YAML edit or a terminal command mid-demo
  DO     model arrival timing inside the mocked source system
  DO     let a plain rematerialize be the entire recovery story

Mock source state lives in `demo_data/`, representing the upstream system's own
state. Dagster reads it and never writes it as part of the demo narrative.
Resetting the demo is an operation on that mock source, run from a script or
make target OUTSIDE Dagster.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import dagster as dg
import pandas as pd
from pydantic import Field

# The real component, installed from the community registry via:
#   uvx --from dagster-community-components-cli dagster-component add snowflake_query
from components.ingestion.snowflake_query import SnowflakeQueryComponent

from demo_data.generators import generate_frame_for_query

# The mocked source system's own state. NOT Dagster state, NOT a demo toggle --
# this stands in for "what the upstream API currently has". Kept out of the
# project package so it is obviously not part of the pipeline.
_SOURCE_STATE = Path(__file__).parent.parent / "demo_data" / "_source_state.json"


class DemoSnowflakeQueryComponent(SnowflakeQueryComponent):
    """`SnowflakeQueryComponent` that reads a simulated source instead of Snowflake.

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
            "Read from the simulated source instead of querying Snowflake. "
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
    demo_late_partitions: list[str] = Field(
        default_factory=list,
        description=(
            "Partition keys the simulated source has NOT yet received. The first "
            "read of one of these returns nothing, so the blocking check fails. "
            "The source then 'receives' the data, and a plain rematerialize "
            "succeeds -- no heal step, because assets are idempotent and it is "
            "the source that changed."
        ),
    )

    def _execute_query(
        self,
        context: dg.AssetExecutionContext,
        query: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Return query results, from the simulated source when `demo_mode` is set.

        This is the entire seam. `_execute_query` is the innermost method that
        actually opens a Snowflake connection; everything upstream of it --
        asset construction, partition mapping, metadata emission -- is the real
        component's code and runs identically in both modes.

        When adapting this to a different component, find the equivalent single
        method and override only that one. If the component has no such seam,
        add one by extracting the network call into its own method and upstream
        that refactor to the registry rather than working around it here.
        """
        if not self.demo_mode:
            return super()._execute_query(context, query, **kwargs)

        partition_key = context.partition_key if context.has_partition_key else None

        if partition_key is not None and not self._source_has(partition_key):
            # The upstream system genuinely has nothing yet. Return an empty
            # frame with the right schema so the blocking check fails on real
            # emptiness rather than on a special-cased demo branch.
            self._mark_source_received(partition_key)
            context.log.info(
                "Simulated source has no rows for %s yet (late feed). "
                "The data lands after this read; rematerialize to pick it up.",
                partition_key,
            )
            return generate_frame_for_query(
                query=query,
                row_count=0,
                seed=self.demo_seed,
                partition_key=partition_key,
            )

        frame = generate_frame_for_query(
            query=query,
            row_count=self.demo_row_count,
            seed=self.demo_seed,
            partition_key=partition_key,
        )

        # Mirror the metadata the real path emits, so the UI looks identical.
        context.add_output_metadata(
            {
                "dagster/row_count": len(frame),
                "source": dg.MetadataValue.text(
                    "simulated -- set demo_mode: false in defs.yaml to query Snowflake"
                ),
            }
        )
        return frame

    # -- simulated source system state ------------------------------------
    # This models what the upstream API has, not anything about Dagster. The
    # asset stays idempotent: it reads the source and reports what it finds.

    def _source_has(self, partition_key: str) -> bool:
        if partition_key not in self.demo_late_partitions:
            return True
        return partition_key in _read_source_state().get("received", [])

    def _mark_source_received(self, partition_key: str) -> None:
        state = _read_source_state()
        received = set(state.get("received", []))
        received.add(partition_key)
        state["received"] = sorted(received)
        _SOURCE_STATE.parent.mkdir(parents=True, exist_ok=True)
        _SOURCE_STATE.write_text(json.dumps(state, indent=2))


def _read_source_state() -> dict[str, Any]:
    if not _SOURCE_STATE.exists():
        return {}
    try:
        return json.loads(_SOURCE_STATE.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


# ---------------------------------------------------------------------------
# Corresponding defs.yaml -- flipping the demo off is one line.
#
#   type: demo_components.DemoSnowflakeQueryComponent
#   attributes:
#     demo_mode: true                     # <-- the only thing that changes
#     demo_seed: 20260824
#     demo_row_count: 50000
#     demo_late_partitions: ["2026-08-21"]
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
#
# Demo flow, with no demo-only objects anywhere in the graph:
#   1. Materialize everything. The late partition comes back empty; the
#      blocking check fails; downstream refuses to compute.
#   2. Rematerialize just that partition. The source now has the data.
#      Downstream recomputes via its automation condition. Graph goes green.
#
# Reset for the next demo (outside Dagster, never a Dagster object):
#   make reset-demo      ->    rm -f demo_data/_source_state.json
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
# component is completely unmodified -- the strongest version of this pattern,
# and the one to reach for first.
# ---------------------------------------------------------------------------
