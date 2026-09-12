"""Deterministic stub rows for the four business-unit raw data products.

Per the brief's graph-first Fidelity directive plus CLAUDE.md's Axis 2
("stubbed... trivial content, not real business calculation"), these are a
handful of deterministic, seeded placeholder values -- not a full synthetic-
data-generation apparatus. What matters for Axis 1 is that real rows land in
the shared DuckDB warehouse (so the blocking completeness/freshness checks
and the dbt staging layer downstream have something real to query), not that
the numbers mean anything to Umicore's actual production volumes -- none
were given by the AE, and none are invented here.

Two raw tables (`raw.cathode_production_batch`, `raw.ac_daily_feed`) are
"produced" by the demo-mode Databricks workspace component, standing in for
the real PySpark/Databricks jobs that would write them in production. The
other two (`raw.recycling_batch_yield`, `raw.specialty_materials_output`)
have no specific ingestion tool named in the AE notes (marked `unknown` in
the brief's stack table), so they're written by the generic
`WarehouseTableAssetsComponent` stub instead -- same deterministic-row
convention, no vendor claimed.

Same asset key + same partition date always produce the same rows, so
repeated demo runs (and validate_e2e.py) never see counts drift.
"""

import hashlib

import pandas as pd


def _seed_for(table_name: str, partition_date: str) -> int:
    digest = hashlib.sha256(f"{table_name}:{partition_date}".encode()).hexdigest()
    return int(digest, 16) % 1_000_000


def stub_dataframe(table_name: str, partition_date: str, row_count: int) -> pd.DataFrame:
    """Return a small, deterministic, plausible DataFrame for the given raw table."""
    seed = _seed_for(table_name, partition_date)

    if table_name == "raw.cathode_production_batch":
        lines = ["line_a", "line_b", "line_c"]
        grades = ["battery_grade_ni_high", "battery_grade_ni_standard"]
        data = {
            "cathode_batch_id": [seed + i for i in range(row_count)],
            "production_line": [lines[i % len(lines)] for i in range(row_count)],
            "material_grade": [grades[i % len(grades)] for i in range(row_count)],
            "output_kg": [round(800 + ((seed + i * 37) % 4500) / 10, 1) for i in range(row_count)],
            "produced_on": [partition_date] * row_count,
        }
    elif table_name == "raw.ac_daily_feed":
        units = ["ac_unit_1", "ac_unit_2", "ac_unit_3", "ac_unit_4"]
        data = {
            "feed_record_id": [seed + i for i in range(row_count)],
            "catalyst_unit_id": [units[i % len(units)] for i in range(row_count)],
            "conversion_rate_pct": [round(88 + ((seed + i * 13) % 1100) / 100, 2) for i in range(row_count)],
            "throughput_units": [500 + ((seed + i * 59) % 3000) for i in range(row_count)],
            "recorded_on": [partition_date] * row_count,
        }
    elif table_name == "raw.recycling_batch_yield":
        lines = ["black_mass_line_1", "black_mass_line_2"]
        materials = ["battery_scrap", "catalyst_scrap", "process_offcuts"]
        data = {
            "batch_id": [seed + i for i in range(row_count)],
            "recycling_line": [lines[i % len(lines)] for i in range(row_count)],
            "input_material": [materials[i % len(materials)] for i in range(row_count)],
            "recovered_material_kg": [round(200 + ((seed + i * 71) % 2200) / 10, 1) for i in range(row_count)],
            "yield_pct": [round(60 + ((seed + i * 17) % 3200) / 100, 2) for i in range(row_count)],
            "processed_on": [partition_date] * row_count,
        }
    elif table_name == "raw.specialty_materials_output":
        lines = ["cobalt_specialty_line", "germanium_specialty_line"]
        grades = ["spec_a", "spec_b", "spec_c"]
        data = {
            "output_id": [seed + i for i in range(row_count)],
            "product_line": [lines[i % len(lines)] for i in range(row_count)],
            "quality_grade": [grades[i % len(grades)] for i in range(row_count)],
            "output_kg": [round(150 + ((seed + i * 43) % 1800) / 10, 1) for i in range(row_count)],
            "produced_on": [partition_date] * row_count,
        }
    else:
        raise ValueError(f"no stub schema defined for {table_name!r}")

    return pd.DataFrame(data)
