# Demo script -- One Definition, 170 Tenants

Run locally: `dg dev`. Dagster+ points are marked **[Dagster+]**; conversation-only items **[talk]**.

1. **Open the asset graph.** "Fivetran lands the client data, dbt builds four layers, Sigma reads the marts. 16 assets here, 20 demo tenants, from one definition each." Left side: the SSIS group -- "your legacy loads, observed next to the new path. We don't have to decide today whether they stay."
2. **Click a Fivetran asset.** Metadata panel is the script: owner, tier, SLA, source system, kinds `fivetran` + `snowflake`.
3. **Open the partitions view.** Date x tenant grid. "This is the answer to 170 databases: one definition, one partition per client per day."
4. **Materialize one partition** (e.g. `tenant_001`, 2026-10-01, select Fivetran through the mart). Watch landing, then dbt layers, then checks.
5. **Show the checks tab.** Landed row-count (blocking), published completeness (blocking), SLA-ready (warning), dbt tests. "If Fivetran lands 98% of the rows, dbt does not run. Today your customers find that out first." (Talk through; nothing is staged.)
6. **Rerun just that tenant.** "Selective rerun: one client, one day, nothing else touched." Row counts are identical -- the asset is idempotent.
7. **Lineage for one tenant:** Fivetran -> dbt ingest -> transform -> published -> mart -> Sigma workbook.
8. **Freshness + automation:** point at the mart's freshness policy and the eager, check-gated automation. "Declarative: you say what should be fresh, not when to run." Show the nightly schedule.
9. **[Dagster+] Alerting:** "A failed check or stale tenant goes to Slack/Teams/email through alert policies -- configured in the UI, no code." **[Dagster+]** branch deployments for "a change reaches all tenants", restart from failure, RBAC.
10. **Jean's question, "why not Terraform?"** **[talk]** Terraform stands infrastructure up; it doesn't tell you which tenant is stale or rerun the broken step. Dagster has a Terraform provider -- they complement each other.
11. **Adding the next client:** one entry in `TENANTS` -- show `partitions.py`. Close: "You'd know which tenant is stale before they do."

Conversation only: credit estimate at 170 tenants (confirm with deal desk), POC scope (2-3 tenants, Fivetran + dbt), phData tooling.
