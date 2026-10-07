"""Demo-mode seam on top of the REAL `dagster_fivetran.FivetranAccountComponent`.

Nothing here reimplements Fivetran. The component, its asset specs, its
`execute()` sync-and-poll, and its polling sensor all run the unmodified
`dagster-fivetran` code. Two things differ in demo mode, both at the network
boundary:

* `DemoFivetranWorkspace.get_client()` returns `FakeFivetranClient`
  (`demo_data/fivetran_api.py`) instead of an HTTP client, so discovery, the
  sync call, and the sensor's completion poll all read synthetic API
  responses.
* `execute()` first lands the partition's rows into the local DuckDB
  warehouse. Real Fivetran loads the destination itself, with no Dagster code
  on that path; this is the stand-in for "the sync finished and the tables
  are there". Set `demo_mode: false` and it is skipped.

Asset keys, specs, partitions (applied from the YAML `translation:` block),
dependency edges, and the YAML schema are identical in both modes.
"""

from collections.abc import Iterable
from typing import Annotated

import dagster as dg
from dagster._annotations import public
from dagster.components.resolved.base import resolve_fields
from dagster_fivetran import FivetranAccountComponent, FivetranWorkspace
from dagster_fivetran.resources import FivetranClient
from pydantic import Field

from skygen.demo_data.fivetran_api import FakeFivetranClient
from skygen.demo_data.landing import land_slice
from skygen.partitions import split_partition_key


class DemoFivetranWorkspace(FivetranWorkspace):
    """`FivetranWorkspace` whose API client is simulated when `demo_mode` is on."""

    demo_mode: bool = Field(
        default=True,
        description=(
            "Serve Fivetran API responses from a simulated account instead of "
            "calling api.fivetran.com. Set false and supply real "
            "`account_id`/`api_key`/`api_secret` to run against Skygen's account."
        ),
    )

    def get_client(self) -> FivetranClient:
        if self.demo_mode:
            return FakeFivetranClient()  # type: ignore[return-value]
        return super().get_client()


def _resolve_demo_workspace(context: dg.ResolutionContext, model) -> DemoFivetranWorkspace:
    return DemoFivetranWorkspace(**resolve_fields(model, DemoFivetranWorkspace, context))  # type: ignore[arg-type]


class DemoFivetranAccountComponent(FivetranAccountComponent):
    """`FivetranAccountComponent` with a demo-mode API client and landing seam."""

    workspace: Annotated[
        DemoFivetranWorkspace,
        dg.Resolver(_resolve_demo_workspace),
    ]

    @public
    def execute(
        self, context: dg.AssetExecutionContext, fivetran: FivetranWorkspace
    ) -> Iterable[dg.AssetMaterialization | dg.MaterializeResult]:
        if isinstance(fivetran, DemoFivetranWorkspace) and fivetran.demo_mode:
            tenant, day = split_partition_key(context.partition_key)
            landed = land_slice(tenant, day)
            context.log.info(f"[demo mode] landed {landed} for {tenant} on {day}")
        yield from super().execute(context, fivetran)
