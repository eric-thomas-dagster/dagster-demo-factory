"""Shared partition definitions.

One daily grain for the whole target-lakehouse chain (`raw_price_feed`,
`raw_corporate_actions`, `barrons_400_constituents`, `licensee_distribution_extract`,
`daily_coverage_summary`) -- the brief's POC scope is "one index end-to-end"
scored once a day, with no second partition dimension named anywhere in the
brief (no region/tenant/carrier axis exists for a single-index scoring run).

The legacy `legacy_sql_agent_*` assets are intentionally unpartitioned --
`SsisWorkspaceComponent` (the real registry component behind them) has no
`partitions_def` field and its polling sensor emits unpartitioned
`AssetObservation`s (see `components/demo_ssis_workspace.py` and
`component-feedback/2026-09-24-ssis-workspace-gaps.md`).
"""

import dagster as dg

DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-09-01",
    timezone="America/New_York",
)
