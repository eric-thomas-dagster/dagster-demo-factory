# Braze customer-segment export -- no registry or native component

**Date:** 2026-09-03
**Build:** `demos/rvu-tempcover` ("From Ran to Right")

## What was needed

An asset representing a daily customer-segment export from the warehouse
to Braze (a marketing-automation / customer-engagement platform) for
activation campaigns -- e.g. re-engaging customers who abandoned a quote.
One asset, downstream of a single daily fact table; not a workspace of
many Braze objects to enumerate.

## What was searched

```
dagster-component search "braze" --json
dagster-component search "customer engagement marketing activation" --json
dagster-component search "braze customer segment export" --json
```

All three returned `[]` (zero hits, no partial matches on any field). No
native `dagster-braze` package exists either (not on PyPI as of this
build; `uv add dagster-braze` was not attempted since `dagster-component
search` and the registry's own category listing gave no indication one
exists, and CLAUDE.md's registry inventory rule blocks asserting a gap
without a search -- searched, not guessed).

## What came closest

Nothing in the registry names Braze, or a close generic equivalent (no
"customer data platform" / "marketing activation" category component
either). The closest adjacent shape in the registry is the
`http_external_asset` generic HTTP-trigger wrapper (matched on unrelated
terms during the Fivetran search), which could technically POST to
Braze's REST API, but it's a generic HTTP-runner primitive, not a
Braze-specific component with typed config for Braze's actual endpoints
(user track / catalog / campaign trigger), so it wasn't reached for here
--CLAUDE.md's ladder treats a domain-specific component as the right rung
before a generic HTTP wrapper, and none exists for this domain.

## What was built instead

A plain, demo-mode-mocked `@dg.asset` --
`src/rvu_tempcover/defs/activation/braze_customer_segment_export.py`.
Reads a real row count from `fct_quotes_daily` (materialized for real by
dbt) and simulates the Braze POST; a `DEMO_MODE` flag documents the real
network call it would make (`BRAZE_API_KEY` / `BRAZE_REST_ENDPOINT`
against `/users/track` or a catalog/custom-attribute endpoint) without
implementing it, since no real Braze account exists for this build.

Per the brief's explicit fallback ("a plain demo-mode-mocked `@asset` --
if nothing turns up, that's a `component-feedback/` entry") and
`CLAUDE.md`'s rung 4 guidance, this one-object case didn't warrant a full
custom component built to the workspace-component convention (no
enumerate/execute/observe triad to generalize -- there is exactly one
Braze destination in this domain, not many objects to discover).

## Suggested change

A `braze_workspace` (or `braze_segment_export`) component following the
same Fivetran/PowerBI-shape convention: a `workspace:` block
(`BRAZE_API_KEY` + `BRAZE_REST_ENDPOINT`, both region-specific in Braze's
real API), a `segments:` or `exports:` mapping table (segment name ->
asset key + source query), and a compute body that POSTs each partition's
export to Braze's `/users/track` (attributes) or `/catalogs` endpoint.
Braze's REST API is well-documented and stable, so this would be a
reasonably scoped addition for a prospect that names Braze as often as
this one's stack does (job-posting-confirmed for RVU/Tempcover).
