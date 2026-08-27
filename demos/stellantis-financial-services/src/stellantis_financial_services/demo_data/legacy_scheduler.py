"""Mock of SFS's own homegrown scheduler -- the actual system of record for
the two feeds that haven't cut over to Fabric yet (`raw_dealer_floorplan_feed`,
`raw_credit_bureau_pull`). No Fabric pipeline and no Dagster asset compute
stands behind either one; SFS's own scheduler runs the SSIS package and lands
it into shared warehouse storage on its own timeline. Dagster only ever
*observes* that fact -- see `defs/legacy_assets/legacy_assets.py`.

`ensure_legacy_data_landed` is the one place that shared storage gets
populated. Two kinds of callers reach it, both representing something
reading a lakehouse table someone else already wrote to, never Dagster
producing the asset itself:

- `legacy_scheduler_observer` (the dedicated sensor), to know what to report
  as observed and with what arrival timing.
- `dim_dealer` / `dim_borrower` (the Fabric-migrated assets one step
  downstream of the legacy/Fabric boundary), the same way they'd read a
  shared lakehouse table in production regardless of which system wrote it.

It is idempotent -- a partition already landed is left alone -- so it's safe
to call from every reader without double-writing or drifting between runs.
Nothing under `raw_dealer_floorplan_feed` or `raw_credit_bureau_pull`'s own
asset key ever executes as a Dagster computation.
"""

import hashlib

from stellantis_financial_services.demo_data.generators import (
    generate_credit_bureau_pull_frame,
    generate_dealer_floorplan_feed_frame,
)
from stellantis_financial_services.demo_data.warehouse import upsert_partition

# Independent of DemoFabricWorkspaceComponent's `demo_seed` (defs.yaml) --
# SFS's own scheduler isn't a component and has no YAML config surface (see
# the brief's Component strategy: this is core Dagster capability, not a
# rung-4 custom component). Fixed here so the legacy feeds are deterministic
# regardless of what the Fabric side's seed is configured to.
_LEGACY_SEED = 20260826

_DDL_COLUMNS = {
    "raw_dealer_floorplan_feed": {
        "dealer_id": "VARCHAR", "dealer_group": "VARCHAR", "feed_date": "VARCHAR",
        "units_floored": "BIGINT", "floorplan_balance": "DOUBLE",
        "curtailment_due_amount": "DOUBLE", "arrival_hour": "BIGINT",
    },
    "raw_credit_bureau_pull": {
        "pull_id": "VARCHAR", "borrower_id": "VARCHAR", "bureau_name": "VARCHAR",
        "pull_date": "VARCHAR", "credit_score": "BIGINT",
    },
}


def _match_for(asset_name: str, event_date: str, dealer_group: str | None) -> dict[str, str]:
    if asset_name == "raw_dealer_floorplan_feed":
        return {"feed_date": event_date, "dealer_group": dealer_group}
    return {"pull_date": event_date}


def _already_landed(conn, asset_name: str, match: dict[str, str]) -> bool:
    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = ?",
        [asset_name],
    ).fetchone()[0]
    if not exists:
        return False
    where_clause = " AND ".join(f"{col} = ?" for col in match)
    return conn.execute(f"SELECT count(*) FROM raw.{asset_name} WHERE {where_clause}", list(match.values())).fetchone()[0] > 0


def ensure_legacy_data_landed(conn, asset_name: str, event_date: str, dealer_group: str | None = None) -> int:
    """Idempotent: lands one partition of SFS's own-scheduler output into the
    shared warehouse if it isn't already there. Returns the row count present
    for that partition (0 rows generated, or the count already landed)."""
    match = _match_for(asset_name, event_date, dealer_group)
    if not _already_landed(conn, asset_name, match):
        frame = (
            generate_dealer_floorplan_feed_frame(event_date, dealer_group, _LEGACY_SEED)
            if asset_name == "raw_dealer_floorplan_feed"
            else generate_credit_bureau_pull_frame(event_date, _LEGACY_SEED)
        )
        upsert_partition(conn, "raw", asset_name, frame, match, ddl_columns=_DDL_COLUMNS[asset_name])
    where_clause = " AND ".join(f"{col} = ?" for col in match)
    return conn.execute(f"SELECT count(*) FROM raw.{asset_name} WHERE {where_clause}", list(match.values())).fetchone()[0]


def legacy_completion_metadata(conn, asset_name: str, event_date: str, dealer_group: str | None = None) -> dict:
    """Arrival-timing metadata for the observation sensor to report, read
    back from the already-landed partition rather than invented separately --
    the same `arrival_hour` the lateness check itself evaluates."""
    if asset_name == "raw_dealer_floorplan_feed":
        arrival_hour = conn.execute(
            "SELECT max(arrival_hour) FROM raw.raw_dealer_floorplan_feed WHERE feed_date = ? AND dealer_group = ?",
            [event_date, dealer_group],
        ).fetchone()[0]
        return {"completed_at": f"{event_date}T{arrival_hour:02d}:00:00", "source_system": "SFS homegrown scheduler (SSIS)"}
    # Credit bureau pull has no per-row arrival timestamp in the synthetic
    # data; the nightly bureau batch completes at a stable, deterministic
    # time each night rather than a planted/varying one.
    digest = hashlib.sha256(f"credit_bureau_completion|{event_date}".encode("utf-8")).digest()
    minute = digest[0] % 60
    return {"completed_at": f"{event_date}T05:{minute:02d}:00", "source_system": "SFS homegrown scheduler (SSIS)"}
