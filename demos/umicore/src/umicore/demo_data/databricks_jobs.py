"""Fixed Databricks job/task -> raw-table mapping shared by the demo-mode
discovery seam (`DemoDatabricksWorkspace.fetch_jobs`) and the demo-mode
execution seam (`DemoDatabricksWorkspaceComponent._create_job_asset_def`).

Two jobs stand in for the two business units whose stack table names
Databricks/PySpark explicitly (`battery_materials`, `catalysis` -- the "AC"
unit named in the pain example). `recycling` and `specialty_materials` have
no ingestion tool named in the brief, so they go through the generic
`WarehouseTableAssetsComponent` instead (see that module).
"""

DEMO_JOBS = [
    {
        "job_id": 900101,
        "job_name": "battery_materials_feature_pipeline",
        "task_key": "extract_cathode_production_batch",
        "notebook_path": "/Repos/data-platform/battery_materials/extract_cathode_production_batch",
        "table_name": "raw.cathode_production_batch",
        "partition_column": "produced_on",
    },
    {
        "job_id": 900102,
        "job_name": "catalysis_ac_daily_pipeline",
        "task_key": "extract_ac_daily_feed",
        "notebook_path": "/Repos/data-platform/catalysis/extract_ac_daily_feed",
        "table_name": "raw.ac_daily_feed",
        "partition_column": "recorded_on",
    },
]

TABLE_BY_JOB_TASK = {(j["job_name"], j["task_key"]): j["table_name"] for j in DEMO_JOBS}
PARTITION_COLUMN_BY_TABLE = {j["table_name"]: j["partition_column"] for j in DEMO_JOBS}
