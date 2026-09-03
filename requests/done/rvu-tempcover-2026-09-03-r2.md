---
action: rebuild-demo
requested: 2026-09-03
---

Fivetran, Power BI, and dbt already used real components — keep that. The gap is AZURE DATA FACTORY: it has no component at all, only prose mentions, despite azure_data_factory existing in the registry. ADF is the incumbent the whole thesis is built against ('ADF can tell you a job ran; this shows whether the data was right') so it must be a first-class component with observation, not narrative. Also remove GraphFirstAssetsComponent. Report the full system-to-component-ID mapping.
