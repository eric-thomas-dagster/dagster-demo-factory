"""Deterministic synthetic data generators.

Every function here stands in for a real system Northwind actually runs:

- `generate_carrier_rate_frame`   -> FedEx / UPS / regional-LTL rate APIs
- `generate_shipment_events_frame` -> the homegrown TMS that emits shipment events
- `generate_salesforce_accounts_frame` -> Salesforce, via Fivetran
- `generate_zendesk_tickets_frame`     -> Zendesk, via Fivetran
- `generate_netsuite_gl_entries_frame` -> NetSuite, via Fivetran

All generation is seeded from `(base_seed, partition_key, ...)` so the same
inputs always produce the same rows -- required by the non-negotiable that
repeat demo runs must not drift. Volumes are scaled down from Northwind's
real numbers (brief: ~4M shipment events/day) to a size that stays fast to
materialize while keeping realistic cardinalities and skew.
"""

from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

CARRIERS = ["fedex", "ups", "regional_ltl_a", "regional_ltl_b"]

LANES = [
    ("ORD", "DFW"),
    ("ORD", "ATL"),
    ("LAX", "PHX"),
    ("LAX", "SEA"),
    ("JFK", "BOS"),
    ("JFK", "MIA"),
    ("DFW", "DEN"),
    ("ATL", "MIA"),
    ("SEA", "DEN"),
    ("PHX", "DFW"),
]

# Each carrier services a fixed subset of lanes -- mirrors how a mid-market
# 3PL splits freight across national (FedEx/UPS) and regional LTL carriers.
CARRIER_LANES = {
    "fedex": LANES,
    "ups": LANES,
    "regional_ltl_a": LANES[:6],
    "regional_ltl_b": LANES[4:],
}

CUSTOMERS = [
    {"customer_id": f"CUST-{i:03d}", "segment": segment, "region": region}
    for i, (segment, region) in enumerate(
        [
            ("enterprise", "midwest"),
            ("enterprise", "west"),
            ("enterprise", "northeast"),
            ("mid_market", "south"),
            ("mid_market", "west"),
            ("mid_market", "midwest"),
            ("mid_market", "northeast"),
            ("smb", "south"),
            ("smb", "west"),
            ("smb", "midwest"),
            ("enterprise", "south"),
            ("mid_market", "northeast"),
            ("smb", "northeast"),
            ("enterprise", "midwest"),
            ("smb", "south"),
            ("mid_market", "south"),
            ("enterprise", "west"),
            ("smb", "midwest"),
            ("mid_market", "west"),
            ("enterprise", "northeast"),
            ("smb", "west"),
            ("mid_market", "midwest"),
            ("enterprise", "south"),
            ("smb", "northeast"),
            ("mid_market", "northeast"),
        ],
        start=1,
    )
]

CUSTOMER_NAME_STEMS = [
    "Meridian",
    "Prairie",
    "Cascade",
    "Summit",
    "Harborline",
    "Ironwood",
    "Bluepeak",
    "Redwood",
    "Northfield",
    "Lakeshore",
    "Copperline",
    "Fairview",
    "Granite",
    "Silverton",
    "Crestwood",
    "Amberfield",
    "Stonebridge",
    "Westgate",
    "Clearwater",
    "Eastport",
    "Millbrook",
    "Ashford",
    "Riverdale",
    "Highpoint",
]

# Peak-season gesture: a couple of partitions inside the demo window run
# ~3x normal shipment volume, without modeling a full Oct-Dec seasonal curve.
PEAK_VOLUME_DATES = {"2026-08-17", "2026-08-23"}


def _stable_seed(*parts: str | int) -> int:
    """Combine a base seed with partition context into one deterministic int.

    Python's built-in `hash()` is randomized per-process (PYTHONHASHSEED),
    so it cannot be used here -- a run-to-run stable digest is required.
    """
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def generate_carrier_rate_frame(event_date: str, carrier: str, seed: int) -> pd.DataFrame:
    """Stands in for a FedEx/UPS/regional-LTL rate API pull for one carrier, one day."""
    rng = np.random.default_rng(_stable_seed(seed, "carrier_rate", event_date, carrier))
    lanes = CARRIER_LANES[carrier]
    base_rate = {"fedex": 2.35, "ups": 2.28, "regional_ltl_a": 1.95, "regional_ltl_b": 1.88}[carrier]
    rows = [
        {
            "carrier": carrier,
            "event_date": event_date,
            "lane": f"{origin}-{destination}",
            "rate_per_mile": round(float(base_rate + rng.normal(0, 0.08)), 4),
            "fuel_surcharge_pct": round(float(rng.uniform(0.09, 0.16)), 4),
            "quoted_at": f"{event_date}T06:{rng.integers(0, 59):02d}:00Z",
        }
        for origin, destination in lanes
    ]
    return pd.DataFrame(rows)


def generate_shipment_events_frame(event_date: str, seed: int) -> pd.DataFrame:
    """Stands in for the homegrown TMS shipment-event feed for one day.

    Real volume is ~4M/day; scaled down ~15,000x here to stay fast in a demo
    while keeping realistic per-lane, per-carrier, per-customer skew.
    """
    rng = np.random.default_rng(_stable_seed(seed, "shipment_events", event_date))
    base_count = int(rng.integers(220, 320))
    count = base_count * 3 if event_date in PEAK_VOLUME_DATES else base_count

    lane_idx = rng.integers(0, len(LANES), size=count)
    carrier_choices = rng.choice(CARRIERS, size=count, p=[0.32, 0.30, 0.19, 0.19])
    customer_choices = rng.choice([c["customer_id"] for c in CUSTOMERS], size=count)
    lane_miles = {
        ("ORD", "DFW"): 800,
        ("ORD", "ATL"): 590,
        ("LAX", "PHX"): 370,
        ("LAX", "SEA"): 1130,
        ("JFK", "BOS"): 215,
        ("JFK", "MIA"): 1280,
        ("DFW", "DEN"): 780,
        ("ATL", "MIA"): 660,
        ("SEA", "DEN"): 1020,
        ("PHX", "DFW"): 890,
    }

    rows = []
    for i in range(count):
        origin, destination = LANES[lane_idx[i]]
        miles = lane_miles[(origin, destination)] + int(rng.integers(-15, 16))
        rows.append(
            {
                "shipment_id": f"SHP-{event_date.replace('-', '')}-{i:05d}",
                "event_date": event_date,
                "carrier": carrier_choices[i],
                "lane": f"{origin}-{destination}",
                "origin": origin,
                "destination": destination,
                "customer_id": customer_choices[i],
                "weight_lbs": round(float(rng.uniform(80, 2400)), 1),
                "miles": miles,
                "status": "delivered",
                "shipped_at": f"{event_date}T{rng.integers(4, 22):02d}:{rng.integers(0, 59):02d}:00Z",
            }
        )
    return pd.DataFrame(rows)


def generate_salesforce_accounts_frame(seed: int) -> pd.DataFrame:
    """Stands in for the Salesforce accounts object, via Fivetran."""
    rng = np.random.default_rng(_stable_seed(seed, "salesforce_accounts"))
    rows = [
        {
            "customer_id": customer["customer_id"],
            "customer_name": f"{CUSTOMER_NAME_STEMS[i % len(CUSTOMER_NAME_STEMS)]} Logistics",
            "segment": customer["segment"],
            "region": customer["region"],
            "created_at": f"202{int(rng.integers(2, 6))}-{int(rng.integers(1, 13)):02d}-01",
        }
        for i, customer in enumerate(CUSTOMERS)
    ]
    return pd.DataFrame(rows)


def generate_zendesk_tickets_frame(seed: int, window_start: str, window_end: str) -> pd.DataFrame:
    """Stands in for the Zendesk tickets object, via Fivetran."""
    rng = np.random.default_rng(_stable_seed(seed, "zendesk_tickets"))
    dates = pd.date_range(window_start, window_end, freq="D").strftime("%Y-%m-%d")
    n_tickets = 60
    ticket_dates = rng.choice(dates, size=n_tickets)
    customer_choices = rng.choice([c["customer_id"] for c in CUSTOMERS], size=n_tickets)
    subjects = [
        "Missing invoice for last week's shipments",
        "Margin report doesn't match our numbers",
        "Shipment shows delivered but no invoice yet",
        "Rate discrepancy on regional LTL lane",
        "Question about carrier surcharge",
    ]
    rows = [
        {
            "ticket_id": f"ZD-{i:05d}",
            "customer_id": customer_choices[i],
            "event_date": ticket_dates[i],
            "subject": subjects[i % len(subjects)],
            "status": "closed" if i % 4 else "open",
            "priority": "high" if i % 5 == 0 else "normal",
        }
        for i in range(n_tickets)
    ]
    return pd.DataFrame(rows)


def generate_netsuite_gl_entries_frame(seed: int, window_start: str, window_end: str) -> pd.DataFrame:
    """Stands in for the NetSuite GL entries object, via Fivetran."""
    rng = np.random.default_rng(_stable_seed(seed, "netsuite_gl_entries"))
    dates = pd.date_range(window_start, window_end, freq="D").strftime("%Y-%m-%d")
    rows = []
    entry_id = 0
    for event_date in dates:
        for customer in CUSTOMERS:
            if rng.random() < 0.55:
                continue
            entry_id += 1
            rows.append(
                {
                    "gl_entry_id": f"GL-{entry_id:06d}",
                    "customer_id": customer["customer_id"],
                    "event_date": event_date,
                    "invoice_id": f"INV-{event_date.replace('-', '')}-{customer['customer_id']}",
                    "amount": round(float(rng.uniform(1800, 42000)), 2),
                    "gl_account": "4000-freight-revenue",
                }
            )
    return pd.DataFrame(rows)
