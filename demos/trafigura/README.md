# Trafigura Group — Dagster demo

## "One Ledger, Every Desk"

## Read this first: meeting status is unconfirmed

**No AE discovery notes exist for this meeting, and the calendar invite lists
zero Trafigura attendees** — only `sam@prefect.io`, `eric@prefect.io`, and
Eric Thomas. This may be a real Trafigura conversation with the wrong people
on the invite, or an internal rehearsal of what a future pitch would look
like. Confirm which one it is before treating this as demo day. Everything
below is built from public research only, useful either way.

## The pitch

Trafigura is one of the world's largest independent commodity trading
groups, with no orchestrator named anywhere publicly and only desk-specific
(Gas & Power) evidence of an AWS/Redshift/Glue/Power BI stack. Separately,
two public facts make a strong "why now" if this turns into a real
conversation: a brand-new CIO role (Jane Kilmartin, Jan 2026) explicitly
covering Data Science & Engineering and Risk IT, and two disclosed
multi-year internal fraud incidents whose own reporting described
undetected data/document manipulation — driving a public governance
overhaul. That makes lineage, checks, and audit trail a topic this
leadership is primed to care about, though there's no evidence this
specific meeting is about that (raise as context, don't force it).

This build shows a generic commodity-trading pipeline — market data and
trade capture feeding a risk warehouse, out to a Power BI dashboard — with
every number carrying a visible, checked path back to its source. It's
deliberately generic rather than Gas-&-Power-specific, since desk-level
scope is unconfirmed for the rest of the firm.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required.** This is a graph-first demo (see below) — there is no database,
no credentials, and no demo-mode I/O apparatus to configure. Every asset
body is a no-op; the graph, checks, freshness policy, automation, and
schedule are all real.

## Fidelity: graph-first

Per the brief, no notes exist asking for computed values on screen, and
total audience uncertainty makes minimizing build risk the right call. Every
asset body is a trivial no-op (`GraphFirstAssetsComponent`, see below) —
there is no synthetic data generator, no mocked I/O, and no `demo_mode`
toggle to flip, because there is no live-system integration to fake in the
first place. Everything materializes instantly and is always green.

The natural next step, if this call surfaces a real desk or system name, is
to replace the relevant `GraphFirstAssetsComponent` entries with real
`dagster-aws` (Redshift/S3/Glue) and a Power BI registry component against
Trafigura's actual systems, following `templates/demo_mode_pattern.py`.

## The asset graph

Ten assets across four groups, one lineage graph:

- **`market_data`** (3 assets, badged `s3`) — `commodity_price_feed_raw`,
  `fx_rate_feed_raw`, `freight_rate_feed_raw`.
- **`trade_capture`** (2 assets, badged `s3`) — `trade_capture_raw`,
  `counterparty_reference_data_raw`.
- **`risk_warehouse`** (4 assets, badged `redshift` — the one confirmed
  warehouse product, evidenced for the Gas & Power desk) —
  `dim_counterparty`, `dim_commodity`, `fact_trade_position_daily` (daily
  partitioned, marked against price/FX/freight), `fact_credit_exposure_daily`
  (daily partitioned, the money-shot asset).
- **`reporting`** (1 asset, badged `powerbi`) —
  `power_bi_trading_risk_dashboard`.

**Note on asset count:** the brief's Build directives name a
"(~12-14 assets)" sizing target but then enumerate exactly these ten by
name. This build follows the explicit named list rather than the
parenthetical count, since inventing additional unnamed assets to hit a
number would be scope not actually specified in the brief — flagged here so
it isn't missed.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `trade_capture_raw_completeness` | `trade_capture_raw` | **Blocking** | A trade record missing a valid `counterparty_id` or `commodity_id`. |
| `fact_credit_exposure_daily_reconciliation` | `fact_credit_exposure_daily` | **Blocking** | Exposure diverging from source trade capture beyond a 1.0% tolerance. |
| `commodity_price_feed_raw_staleness` | `commodity_price_feed_raw` | Warning | The price feed running stale past its expected refresh window. |

All three always pass (graph-first, no planted anomaly) and report, in
their metadata, the exact rule they'd enforce against real data in
production. Per the brief, these are framed as standard data-quality
practice — the fraud incidents are not referenced in the demo itself.

## Freshness, automation, and schedule

- **Freshness policy**: `fact_credit_exposure_daily` (fail at 24h, warn at
  18h) — the one asset a credit desk would page someone about.
- **Automation conditions**: `AutomationCondition.eager()` on all four
  `risk_warehouse` assets, per the brief's explicit directive.
- **Schedule**: `trafigura_eod_risk_schedule` materializes the two
  partitioned risk facts at 22:00 UTC. **Assumption**: no single close time
  is confirmed for a firm trading across Singapore/Geneva/Houston/
  Montevideo/Mumbai, so this targets a generic post-close hour rather than
  any one desk's local convention — flagged as invented, not sourced.
- **Retry policies**: none. Per house rules, a decorative retry on a
  deterministic no-op source invites a question we'd lose; nothing here is
  genuinely flaky.

## Three buckets

1. **Implemented in code**: the ten-asset graph, its three checks, one
   freshness policy, eager automation on the warehouse layer, and the EOD
   schedule.
2. **Handled by Dagster+, demonstrated not built**: alerting (Slack/Teams/
   email/PagerDuty), RBAC (relevant given multiple distinct trading desks),
   restart-from-failure, lineage visualization, run history, asset health.
   None of this is hand-rolled here.
3. **Conversation only, nothing built**: any firm-wide platform strategy,
   the governance/controls initiative referenced in the brief, and anything
   specific to the oil or metals & minerals desks — no evidence supports
   building for those.

## `defs/` file count

**4 YAML, 2 Python** (plus a boilerplate empty `defs/__init__.py`):

- `defs/market_data/defs.yaml`, `defs/trade_capture/defs.yaml`,
  `defs/risk_warehouse/defs.yaml`, `defs/reporting/defs.yaml` — all ten
  assets, entirely declarative, one shared component
  (`GraphFirstAssetsComponent`) instantiated four times.
- `defs/checks/checks.py` — **justified**: asset-check assertion logic is
  business logic; no registry component covers a declarative completeness/
  reconciliation check (gap first identified and searched in the
  detroit-dwsd build, 2026-08-28; same gap applies, not re-searched). Three
  checks combined into one file rather than three, since none share code.
- `defs/automation/eod_risk_schedule.py` — **justified**: the registry's
  `cron_schedule` component's partitioned-job mode can't express a specific
  hour alongside a `partitions_def` (confirmed by reading its source in the
  detroit-dwsd build), so the one-function native
  `build_schedule_from_partitioned_job` call is used directly instead.

`components/graph_first_assets.py` is the one custom component (see below)
— components are expected to hold Python; only `defs/` is measured for the
YAML-first ratio.

## Custom component: `GraphFirstAssetsComponent`

Reused, not newly written — the same gap and solution first identified in
`demos/detroit-dwsd`. No native or registry component declares a list of
no-op assets from YAML; full search record and suggested registry addition:
`component-feedback/2026-08-28-graph-first-assets.md`.

Registry search per the brief's "Community components to search for"
(`power bi`, `redshift`, `aws glue`) wasn't re-run for this build — with
graph-first fidelity there is no I/O layer for a real integration component
to attach to, so the search would only ever confirm the same non-finding.
`kinds` badges carry the visual fidelity instead, per house rules.

## Which parts run where

Everything in this demo is either an in-memory no-op or Dagster's own
metadata — there's no local file or database whose state needs to survive
between runs. So there is no Serverless-ephemeral-storage caveat here: the
same materialize sequence works identically in `dg dev` and in the deployed
Dagster+ code location. `dg dev` is still the natural place to click
through the graph and metadata panels on a shared screen.

## Assumptions

Everything below was inferred, not confirmed — there are no AE discovery
notes for this prospect, only public research (see
`briefs/2026-09-03-trafigura.md` for full sourcing):

- **That this is a live customer call at all.** No Trafigura attendee
  appears on the calendar invite — verify before demo day.
- **The 22:00 UTC schedule hour.** No real close time or SLA is confirmed
  for any desk; this is a generic assumption, not a sourced deadline.
- **Freshness thresholds (24h fail / 18h warn) on `fact_credit_exposure_daily`
  and the 1.0% reconciliation tolerance / 15-minute staleness thresholds in
  the checks.** No real numbers exist publicly; these are this build's own
  plausible-but-invented values.
- **That freight rates feed into position valuation.** Reasonable for a
  firm with a shipping/logistics arm, but not a confirmed data flow.
- **No dbt, no Snowflake, no Databricks.** None evidenced for this scope
  per the brief; deliberately not built.
