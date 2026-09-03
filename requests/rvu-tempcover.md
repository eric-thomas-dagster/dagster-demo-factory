---
action: rebuild-demo
requested: 2026-09-03
---

Previous build routed Fivetran, Azure Data Factory, and Power BI through a home-made GraphFirstAsset component. Use the REAL components for every system: fivetran_assets / fivetran_sync_sensor / fivetran_sync_trigger_job, azure_data_factory, and native dagster-powerbi. Subclass and mock the I/O seam where credentials are missing — never substitute. Report the system-to-component-ID mapping.
