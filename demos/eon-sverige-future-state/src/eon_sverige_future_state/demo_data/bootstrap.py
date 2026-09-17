"""Loads the demo's synthetic fixture data into the local DuckDB warehouse's
`bronze` schema, idempotently, at Definitions-load time.

Stands in for what Databricks bronze Delta tables would look like after the
ingestion Databricks jobs have run -- it is *not* a Dagster asset and has
no lineage node, because in production the Databricks jobs put this data
there (via the Landis+Gyr Gridstream Connect ingestion path for meter
reads, and the grid SCADA feed for grid telemetry). The Databricks jobs
`ingest_meter_reads_to_bronze` / `ingest_grid_telemetry_to_bronze` /
`ingest_customer_records_to_bronze` (see `defs/databricks/defs.yaml`)
represent those pipelines as first-class Dagster assets -- each one's
demo-mode execution reads the rows this module already landed, the same
way a real Databricks job would read rows already sitting in ADLS Gen2
and land them in a bronze Delta table.

Deterministic: seeded PRNG, fixed row counts and schemas, spans three
months so validation partitions (`2026-04-01`, `2026-05-01`) resolve to
non-empty slices. Runs on every process that imports
`eon_sverige_future_state.definitions` -- a fresh clone or a local `dg
dev` all re-populate the same rows.
"""

import datetime
import random

import duckdb

from eon_sverige_future_state.demo_data.warehouse import demo_duckdb_path

_RAW_SEED = 20260904

# Three months of fixture data. Two full months + a partial current month
# so both validation partitions (`2026-04-01`, `2026-05-01`) resolve to
# non-empty slices.
_WINDOW_START = datetime.date(2026, 4, 1)
_WINDOW_END = datetime.date(2026, 6, 30)

# E.ON Sverige's actual Swedish operating regions. Grid telemetry is
# per-region hourly. Meter counts per region are compact for a demo but
# proportioned like the real network (Malmö is E.ON's largest region).
_REGIONS: list[tuple[str, str, int]] = [
    # (region_code, region_name, active_meters)
    ("MALMO", "Malmö", 24),
    ("NORRKOPING", "Norrköping", 18),
    ("UMEA", "Umeå", 12),
]

# Landis+Gyr Gridstream Connect meters serial patterns look like
# "LG-<8 digit>" per their product docs; Swedish customer IDs are
# 10-digit personal numbers (personnummer) rendered without birthdate
# specifics here since this is synthetic data.
_METER_ID_PREFIX = "LG-"
_CUSTOMER_ID_PREFIX = "CUST-SE-"


def _iter_customer_rows() -> list[tuple]:
    """One customer per active meter across the three regions.

    Customer records are subject to EU 2026/855 switching-data
    interoperability. Every field here has an audit purpose downstream in
    `customer_switching_extract`.
    """
    rng = random.Random(_RAW_SEED)
    rows = []
    customer_seq = 0
    switching_states = ["none", "switch_requested", "switch_completed"]
    for region_code, region_name, active_meters in _REGIONS:
        for i in range(active_meters):
            customer_id = f"{_CUSTOMER_ID_PREFIX}{customer_seq:06d}"
            meter_id = f"{_METER_ID_PREFIX}{customer_seq + 10000000:08d}"
            account_open_date = (
                _WINDOW_START
                - datetime.timedelta(days=rng.randrange(30, 720))
            )
            # 90% no switching activity, 8% requested, 2% completed
            switching_status = rng.choices(
                switching_states, weights=[90, 8, 2], k=1
            )[0]
            rows.append(
                (
                    customer_id,
                    meter_id,
                    region_code,
                    region_name,
                    account_open_date,
                    switching_status,
                    # Retailer field matters for EU 2026/855
                    "EON_SE_RETAIL",
                )
            )
            customer_seq += 1
    return rows


def _iter_meter_read_rows(customers: list[tuple]) -> list[tuple]:
    """Daily interval reads per meter across the fixture window.

    kWh values in a plausible Swedish household range (5-45 kWh/day)
    with regional variation and a weekly seasonality pattern.
    """
    rng = random.Random(_RAW_SEED + 1)
    span_days = (_WINDOW_END - _WINDOW_START).days + 1

    rows: list[tuple] = []
    read_seq = 0
    for customer in customers:
        customer_id, meter_id, region_code, _, _, _, _ = customer
        # Regional baseline: Umeå (north) uses more heating
        baseline_kwh = {
            "MALMO": 12.0,
            "NORRKOPING": 14.0,
            "UMEA": 18.0,
        }[region_code]
        for d in range(span_days):
            read_date = _WINDOW_START + datetime.timedelta(days=d)
            # Weekend uplift and monthly variation
            weekday = read_date.weekday()
            weekend_uplift = 1.15 if weekday >= 5 else 1.0
            month_factor = 1.0 + (read_date.month - 5) * 0.05
            kwh = round(
                baseline_kwh
                * weekend_uplift
                * month_factor
                * rng.uniform(0.75, 1.25),
                3,
            )
            rows.append(
                (
                    f"READ-{read_seq:08d}",
                    meter_id,
                    customer_id,
                    region_code,
                    read_date,
                    kwh,
                    # Signal quality: NB-IoT report shape (RSRP dBm, 0-31 CSQ)
                    rng.randint(-115, -85),
                    rng.randint(20, 31),
                )
            )
            read_seq += 1
    return rows


def _iter_grid_telemetry_rows() -> list[tuple]:
    """Hourly grid load telemetry per network region.

    MW values in a plausible Swedish distribution network range for the
    three regions.
    """
    rng = random.Random(_RAW_SEED + 2)
    span_days = (_WINDOW_END - _WINDOW_START).days + 1

    rows: list[tuple] = []
    telemetry_seq = 0
    for region_code, _region_name, _ in _REGIONS:
        # Regional peak load baseline (MW)
        peak_mw = {
            "MALMO": 480.0,
            "NORRKOPING": 320.0,
            "UMEA": 210.0,
        }[region_code]
        for d in range(span_days):
            hour_date = _WINDOW_START + datetime.timedelta(days=d)
            for hour in range(24):
                # Diurnal curve: morning + evening peaks
                diurnal = [
                    0.55, 0.50, 0.48, 0.47, 0.48, 0.55,  # 0-5
                    0.72, 0.88, 0.95, 0.88, 0.82, 0.85,  # 6-11
                    0.92, 0.88, 0.82, 0.82, 0.90, 0.98,  # 12-17
                    1.00, 0.95, 0.88, 0.78, 0.68, 0.60,  # 18-23
                ][hour]
                load_mw = round(
                    peak_mw * diurnal * rng.uniform(0.92, 1.08), 2
                )
                # Voltage stability KPI (nominal 400V ± 10%)
                voltage_v = round(rng.uniform(392.0, 408.0), 1)
                rows.append(
                    (
                        f"TELEM-{telemetry_seq:08d}",
                        region_code,
                        hour_date,
                        hour,
                        load_mw,
                        voltage_v,
                    )
                )
                telemetry_seq += 1
    return rows


_CUSTOMER_COLUMNS = [
    "customer_id",
    "meter_id",
    "region_code",
    "region_name",
    "account_open_date",
    "switching_status",
    "retailer_code",
]

_METER_READ_COLUMNS = [
    "read_id",
    "meter_id",
    "customer_id",
    "region_code",
    "read_date",
    "kwh",
    "signal_rsrp_dbm",
    "signal_csq",
]

_TELEMETRY_COLUMNS = [
    "telemetry_id",
    "region_code",
    "telemetry_date",
    "hour_of_day",
    "load_mw",
    "voltage_v",
]


def ensure_raw_fixtures_loaded() -> None:
    conn = duckdb.connect(demo_duckdb_path())
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

        # Customers: static-per-run panel.
        conn.execute("DROP TABLE IF EXISTS bronze.customer_records")
        conn.execute(
            "CREATE TABLE bronze.customer_records ("
            "customer_id VARCHAR, "
            "meter_id VARCHAR, "
            "region_code VARCHAR, "
            "region_name VARCHAR, "
            "account_open_date DATE, "
            "switching_status VARCHAR, "
            "retailer_code VARCHAR)"
        )
        customers = _iter_customer_rows()
        placeholders = ", ".join(["?"] * len(_CUSTOMER_COLUMNS))
        conn.executemany(
            f"INSERT INTO bronze.customer_records VALUES ({placeholders})",
            customers,
        )

        # Meter reads: seeded per-customer daily reads.
        conn.execute("DROP TABLE IF EXISTS bronze.meter_reads")
        conn.execute(
            "CREATE TABLE bronze.meter_reads ("
            "read_id VARCHAR, "
            "meter_id VARCHAR, "
            "customer_id VARCHAR, "
            "region_code VARCHAR, "
            "read_date DATE, "
            "kwh DOUBLE, "
            "signal_rsrp_dbm INTEGER, "
            "signal_csq INTEGER)"
        )
        placeholders = ", ".join(["?"] * len(_METER_READ_COLUMNS))
        conn.executemany(
            f"INSERT INTO bronze.meter_reads VALUES ({placeholders})",
            _iter_meter_read_rows(customers),
        )

        # Grid telemetry: hourly per region.
        conn.execute("DROP TABLE IF EXISTS bronze.grid_load_telemetry")
        conn.execute(
            "CREATE TABLE bronze.grid_load_telemetry ("
            "telemetry_id VARCHAR, "
            "region_code VARCHAR, "
            "telemetry_date DATE, "
            "hour_of_day INTEGER, "
            "load_mw DOUBLE, "
            "voltage_v DOUBLE)"
        )
        placeholders = ", ".join(["?"] * len(_TELEMETRY_COLUMNS))
        conn.executemany(
            f"INSERT INTO bronze.grid_load_telemetry VALUES ({placeholders})",
            _iter_grid_telemetry_rows(),
        )
    finally:
        conn.close()


if __name__ == "__main__":
    ensure_raw_fixtures_loaded()
    print(f"loaded fixtures into {demo_duckdb_path()}")
