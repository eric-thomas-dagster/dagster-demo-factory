# Qlik Cloud BI-publish integration

## What was needed

An asset (or component) that publishes/reloads a Qlik Cloud app -- the BI
product Noleggiare's discovery notes name (`Qlik Cloud`), used by the
group's shared Finance BI team to read the cross-company consolidated
finance fact.

## What was searched

Run against `dagster-community-components-cli`, `--json`, 2026-09-03:

- `dagster-component search "qlik" --json` -> three hits, all Qlik
  **Compose**, not Qlik **Cloud**/Qlik **Sense**:
  - `qlik_compose_workflow_sensor` (category: sensor) -- polls a Qlik
    Compose workflow's state
  - `qlik_compose_workflow_trigger_job` (category: job) -- triggers a Qlik
    Compose workflow via its REST API
  - `qlik_compose_workspace` (category: integration) -- enumerates Qlik
    Compose workflows/data marts as assets

## What came closest

`qlik_compose_workspace` -- closest by vendor name, but wrong product
within the same vendor. Qlik Compose is a data-warehouse-automation /
ETL-generation tool (competes with Talend/Informatica-style DW automation);
Qlik Cloud and Qlik Sense are Qlik's separate BI/analytics/dashboarding
products. Noleggiare's AE notes and a Tomasi Auto job posting both name
Qlik Cloud/Qlik Sense specifically -- there is no registry coverage for
that product at all, so rungs 1-3 of the escalation ladder don't apply;
nothing in the registry touches this domain.

## What was built instead

`QlikCloudExportComponent`
(`demos/noleggiare/src/noleggiare/components/qlik_cloud_export.py`) --
built to the demo_mode_pattern.py convention rather than a bare `@asset`:
a `demo_mode: bool` field gates the one network call (`_publish`), which
writes a small manifest to `demo_data/qlik_exports/` in demo mode or POSTs
a reload to `POST /api/v1/apps/{app_id}/reload` on a real Qlik Cloud
tenant when `demo_mode: false`. Asset key, deps, group, and metadata are
identical in both modes.

## Suggested change

A `qlik_cloud_app_reload` (or `qlik_cloud_workspace`) component -- POST
`.../api/v1/apps/{id}/reload`, poll `.../reloads/{id}` to terminal status,
surface duration/row-count/error in metadata -- would close this gap for
every Qlik Cloud (not Compose) prospect going forward. Given Qlik Cloud's
REST API shape (app-scoped reload + poll), it would fit the same
Fivetran-shape workspace convention (`polling_sensor` over reload history
for externally-triggered reloads) already used elsewhere in the registry.
