"""Loads the demo's fixture data into the local DuckDB warehouse's `raw`
schema, idempotently, at Definitions-load time.

This stands in for what BigQuery already holding
`bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022` would look
like -- it is *not* a Dagster asset and has no lineage node, because the
brief specifies that ingestion is out of scope: in real mode we read
directly from BigQuery public data via the external_bigquery_table
components (see `defs/bronze/defs.yaml`). This module simply makes those
same rows queryable in demo mode so the real dbt project downstream has
something to read from a local DuckDB.

Deterministic: seeded PRNG, fixed row counts and schemas, spans the fixed
three-month window 2022-01-01 through 2022-03-31 so both validation
partitions (`2022-01-01`, `2022-02-01`) resolve to non-empty slices. Runs
on every process that imports `neighborhood_intelligence_taxi_python.definitions`
-- a fresh clone, a fresh Dagster+ Serverless container, or a local
`dg dev` all re-populate the same deterministic fixture rows.
"""

import datetime
import random
from pathlib import Path

import duckdb

from neighborhood_intelligence_taxi_python.demo_data.warehouse import demo_duckdb_path

_RAW_SEED = 20260904

# Fixture window spans three months so the brief's monthly partitions
# (2022-01-01, 2022-02-01, 2022-03-01) all resolve to non-empty slices.
_WINDOW_START = datetime.date(2022, 1, 1)
_WINDOW_END = datetime.date(2022, 3, 31)
_TARGET_TRIP_ROWS = 5000

# NYC's 265 TLC taxi zones are heavier than the demo needs; a compact
# 20-zone panel across the busiest boroughs is enough for aggregates to
# read as recognizably NYC without dragging a 265-row lookup into the
# fixture. LocationIDs match the real TLC scheme.
_ZONE_FIXTURE: list[tuple[int, str, str, str]] = [
    (4, "Alphabet City", "Manhattan", "Yellow Zone"),
    (12, "Battery Park", "Manhattan", "Yellow Zone"),
    (13, "Battery Park City", "Manhattan", "Yellow Zone"),
    (24, "Bloomingdale", "Manhattan", "Yellow Zone"),
    (41, "Central Harlem", "Manhattan", "Boro Zone"),
    (43, "Central Park", "Manhattan", "Yellow Zone"),
    (48, "Clinton East", "Manhattan", "Yellow Zone"),
    (50, "Clinton West", "Manhattan", "Yellow Zone"),
    (68, "East Chelsea", "Manhattan", "Yellow Zone"),
    (79, "East Village", "Manhattan", "Yellow Zone"),
    (87, "Financial District North", "Manhattan", "Yellow Zone"),
    (100, "Garment District", "Manhattan", "Yellow Zone"),
    (107, "Gramercy", "Manhattan", "Yellow Zone"),
    (132, "JFK Airport", "Queens", "Airports"),
    (138, "LaGuardia Airport", "Queens", "Airports"),
    (161, "Midtown Center", "Manhattan", "Yellow Zone"),
    (162, "Midtown East", "Manhattan", "Yellow Zone"),
    (163, "Midtown North", "Manhattan", "Yellow Zone"),
    (186, "Penn Station/Madison Sq West", "Manhattan", "Yellow Zone"),
    (230, "Times Sq/Theatre District", "Manhattan", "Yellow Zone"),
]

_VENDORS = [1, 2]  # 1 = CMT, 2 = VeriFone -- the two real TLC vendors
_PAYMENT_TYPES = [1, 2]  # 1 = credit card, 2 = cash


def _iter_trip_rows() -> list[tuple]:
    """Deterministic yellow-taxi trip fixture.

    Row shape and column names match the public
    `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022` schema
    -- pickup/dropoff timestamps, LocationIDs, fare/tip/total dollars, etc.
    The demo silvers real column names, not made-up ones.
    """
    rng = random.Random(_RAW_SEED)
    zone_ids = [row[0] for row in _ZONE_FIXTURE]
    span_days = (_WINDOW_END - _WINDOW_START).days + 1

    rows: list[tuple] = []
    for i in range(_TARGET_TRIP_ROWS):
        # Trip_id is not in the real BQ schema; we synthesize one here so
        # downstream uniqueness checks have a natural key to test.
        trip_id = f"TRIP-{i:07d}"
        day_offset = rng.randrange(span_days)
        pickup_date = _WINDOW_START + datetime.timedelta(days=day_offset)
        pickup_hour = rng.choices(
            population=list(range(24)),
            # Weight rush-hour and evening peaks so agg_hourly_demand
            # reads recognizably.
            weights=[1, 1, 1, 1, 1, 2, 4, 8, 10, 7, 5, 5, 6, 6, 7, 8, 9, 10, 9, 8, 6, 5, 3, 2],
            k=1,
        )[0]
        pickup_minute = rng.randrange(60)
        pickup_ts = datetime.datetime.combine(
            pickup_date, datetime.time(hour=pickup_hour, minute=pickup_minute)
        )

        trip_minutes = rng.randint(3, 55)
        dropoff_ts = pickup_ts + datetime.timedelta(minutes=trip_minutes)

        passenger_count = rng.choices([1, 2, 3, 4], weights=[70, 15, 10, 5], k=1)[0]
        trip_distance_miles = round(rng.uniform(0.4, 18.0), 2)
        pickup_zone = rng.choice(zone_ids)
        # 20% chance of same-zone round trips (short rides).
        dropoff_zone = pickup_zone if rng.random() < 0.20 else rng.choice(zone_ids)

        fare = round(3.0 + trip_distance_miles * rng.uniform(2.4, 3.1), 2)
        tip = round(fare * rng.choice([0.0, 0.10, 0.15, 0.20, 0.25]), 2)
        tolls = round(rng.choice([0.0, 0.0, 0.0, 6.55]), 2)
        total = round(fare + tip + tolls + 0.50, 2)  # +$0.50 improvement surcharge

        vendor_id = rng.choice(_VENDORS)
        payment_type = rng.choice(_PAYMENT_TYPES)

        rows.append(
            (
                trip_id,
                vendor_id,
                pickup_ts,
                dropoff_ts,
                passenger_count,
                trip_distance_miles,
                pickup_zone,
                dropoff_zone,
                payment_type,
                fare,
                tip,
                tolls,
                total,
            )
        )
    return rows


_TRIP_COLUMNS = [
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
]

_ZONE_COLUMNS = ["zone_id", "zone_name", "borough", "service_zone"]


def ensure_raw_fixtures_loaded() -> None:
    conn = duckdb.connect(demo_duckdb_path())
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Zones: static fixture panel.
        conn.execute("DROP TABLE IF EXISTS raw.taxi_zone_lookup")
        conn.execute(
            "CREATE TABLE raw.taxi_zone_lookup ("
            "zone_id INTEGER, zone_name VARCHAR, borough VARCHAR, service_zone VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO raw.taxi_zone_lookup VALUES (?, ?, ?, ?)",
            _ZONE_FIXTURE,
        )

        # Trips: seeded generator so row counts don't drift between runs.
        conn.execute("DROP TABLE IF EXISTS raw.yellow_trips")
        conn.execute(
            "CREATE TABLE raw.yellow_trips ("
            "trip_id VARCHAR, "
            "vendor_id INTEGER, "
            "pickup_datetime TIMESTAMP, "
            "dropoff_datetime TIMESTAMP, "
            "passenger_count INTEGER, "
            "trip_distance DOUBLE, "
            "pickup_location_id INTEGER, "
            "dropoff_location_id INTEGER, "
            "payment_type INTEGER, "
            "fare_amount DOUBLE, "
            "tip_amount DOUBLE, "
            "tolls_amount DOUBLE, "
            "total_amount DOUBLE)"
        )
        placeholders = ", ".join(["?"] * len(_TRIP_COLUMNS))
        conn.executemany(
            f"INSERT INTO raw.yellow_trips VALUES ({placeholders})",
            _iter_trip_rows(),
        )
    finally:
        conn.close()


if __name__ == "__main__":
    ensure_raw_fixtures_loaded()
    print(f"loaded fixtures into {demo_duckdb_path()}")
