"""Bronze external asset declarations for E.ON's source-data landing zone.

Three observable AssetSpec-only declarations (no execution body): raw
smart-meter reads landing in ADLS Gen2 from Landis+Gyr Gridstream
Connect (NB-IoT), hourly grid load telemetry per network segment, and
customer records from the retail platform. These are what E.ON's
platform ingests today -- Dagster observes the ADLS objects and the
downstream Databricks ingestion jobs (see defs/databricks/defs.yaml)
land them in bronze Delta tables.

Kept as a `.py` file (not `defs.yaml`) because none of the community
components in the registry emit external ADLS asset specs at a
per-table grain with the metadata shape here -- a two-line YAML shim
around three `AssetSpec` constructors would be strictly more indirect
than the direct code, and the whole file is 20 lines.
"""

import dagster as dg


@dg.definitions
def defs():
    common_metadata = {
        "owner_team": "team:eon-data-platform",
        "tier": "tier_1",
        "storage": "adls_gen2",
        "control_plane_egress": "eu-north-1 (Stockholm)",
        "data_residency": "SE",
    }
    raw_meter_reads = dg.AssetSpec(
        key=dg.AssetKey(["source", "raw_meter_reads"]),
        group_name="source_data",
        kinds={"azure", "adls"},
        owners=["team:eon-data-platform"],
        description=(
            "Daily NB-IoT smart-meter interval reads landing in ADLS Gen2 from "
            "Landis+Gyr Gridstream Connect. ~1M meters as the rollout completes."
        ),
        metadata={
            **common_metadata,
            "domain": "metering",
            "source_system": "Landis+Gyr Gridstream Connect",
            "integration_pattern": "coexistence",
            "business_impact": (
                "Foundational meter feed for E.ON's ~1M smart-meter rollout -- "
                "every downstream billing, grid, and regulator surface reads "
                "from here."
            ),
        },
    )
    raw_grid_load_telemetry = dg.AssetSpec(
        key=dg.AssetKey(["source", "raw_grid_load_telemetry"]),
        group_name="source_data",
        kinds={"azure", "adls"},
        owners=["team:eon-data-platform"],
        description=(
            "Hourly grid load telemetry per Swedish network segment "
            "(Malmö, Norrköping, Umeå). Landed in ADLS Gen2 from grid SCADA."
        ),
        metadata={
            **common_metadata,
            "domain": "grid_operations",
            "source_system": "Grid SCADA",
            "integration_pattern": "coexistence",
            "business_impact": (
                "Grid-load telemetry feeds the operations briefing and the "
                "Azure ML grid-forecast model."
            ),
        },
    )
    raw_customer_records = dg.AssetSpec(
        key=dg.AssetKey(["source", "raw_customer_records"]),
        group_name="source_data",
        kinds={"azure", "adls"},
        owners=["team:eon-data-platform"],
        description=(
            "Customer account records from E.ON's retail platform. Subject "
            "to EU Implementing Regulation 2026/855 (customer-switching "
            "interoperability + auditable access)."
        ),
        metadata={
            **common_metadata,
            "domain": "customer",
            "source_system": "E.ON Retail",
            "integration_pattern": "coexistence",
            "compliance_regulation": "EU_2026_855",
            "business_impact": (
                "Customer records feed the regulator-facing "
                "customer_switching_extract -- this asset sits on the "
                "auditable side of EU 2026/855."
            ),
        },
    )
    return dg.Definitions(
        assets=[raw_meter_reads, raw_grid_load_telemetry, raw_customer_records],
    )
