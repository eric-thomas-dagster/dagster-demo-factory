"""Simulates Bokadirekt's own upstream booking-platform database.

Per CLAUDE.md's "assets are idempotent" rule: mock source state lives
outside Dagster, in `demo_data/`, and Dagster only ever *reads* it --
`raw_bookings_feed` observes this table, dbt's staging layer selects from
it, but nothing in the asset graph writes it. This module is the one place
that does, and it is deliberately not a Dagster object (no asset, no op,
no job) -- it is the stand-in for "the upstream system already has data",
called once at package import (see `bokadirekt/__init__.py`) so a fresh
clone needs no manual seed step.

Deterministic and idempotent: re-running `dg dev` never changes row counts
(seeded on `booking_id`/`specialist_id`, and a no-op once the tables exist),
matching the demo's zero-drift requirement.
"""

from __future__ import annotations

import hashlib
from datetime import date, timedelta

import duckdb

from bokadirekt.demo_data.warehouse import demo_duckdb_path

# 21 days of history, comfortably covering validate_e2e.py's validation
# dates and a live `dg dev` walkthrough. Booking platforms run 7 days a
# week, so no weekday/weekend gap is modeled.
FEED_START_DATE = date(2026, 8, 1)
FEED_DAY_COUNT = 21

SPECIALIST_COUNT = 40
SERVICE_CATEGORIES = ["hair", "nails", "massage", "skincare", "barbering"]
REGIONS = ["stockholm", "goteborg", "malmo", "uppsala"]
BOOKING_STATUSES = ["completed", "completed", "completed", "cancelled", "no_show"]


def _digest(*parts: object) -> int:
    return int(hashlib.sha256(":".join(str(p) for p in parts).encode()).hexdigest(), 16)


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(demo_duckdb_path())


def seed_if_needed() -> None:
    conn = _connect()
    try:
        conn.execute("create schema if not exists raw")
        existing = conn.execute(
            "select table_name from information_schema.tables where table_schema = 'raw'"
        ).fetchall()
        existing_names = {row[0] for row in existing}
        if "raw_specialists" not in existing_names:
            _seed_specialists(conn)
        if "raw_bookings" not in existing_names:
            _seed_bookings(conn)
    finally:
        conn.close()


def _seed_specialists(conn: duckdb.DuckDBPyConnection) -> None:
    rows = []
    for i in range(SPECIALIST_COUNT):
        seed = _digest("specialist", i)
        rows.append(
            (
                i + 1,
                f"specialist_{i + 1:03d}",
                REGIONS[seed % len(REGIONS)],
                SERVICE_CATEGORIES[(seed // 7) % len(SERVICE_CATEGORIES)],
                str(FEED_START_DATE - timedelta(days=180 + (seed % 900))),
            )
        )
    conn.execute(
        """
        create table raw.raw_specialists (
            specialist_id integer,
            specialist_name varchar,
            region varchar,
            service_category varchar,
            onboarded_date varchar
        )
        """
    )
    conn.executemany("insert into raw.raw_specialists values (?, ?, ?, ?, ?)", rows)


def _seed_bookings(conn: duckdb.DuckDBPyConnection) -> None:
    rows = []
    booking_id = 1
    for day_offset in range(FEED_DAY_COUNT):
        booking_date = FEED_START_DATE + timedelta(days=day_offset)
        day_seed = _digest("bookings_per_day", day_offset)
        bookings_today = 30 + (day_seed % 25)  # 30-54 bookings/day
        for i in range(bookings_today):
            seed = _digest("booking", day_offset, i)
            specialist_id = (seed % SPECIALIST_COUNT) + 1
            consumer_id = 5000 + (seed // 13) % 50_000
            status = BOOKING_STATUSES[seed % len(BOOKING_STATUSES)]
            price_sek = 250 + (seed % 45) * 10
            rows.append(
                (
                    booking_id,
                    specialist_id,
                    consumer_id,
                    str(booking_date),
                    status,
                    price_sek,
                )
            )
            booking_id += 1
    conn.execute(
        """
        create table raw.raw_bookings (
            booking_id integer,
            specialist_id integer,
            consumer_id integer,
            booking_date varchar,
            status varchar,
            price_sek integer
        )
        """
    )
    conn.executemany("insert into raw.raw_bookings values (?, ?, ?, ?, ?, ?)", rows)
