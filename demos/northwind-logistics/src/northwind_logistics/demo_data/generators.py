"""Deterministic synthetic data generators standing in for Northwind's real systems.

Every function here is seeded from a base seed plus the partition key being
generated, so the same partition always produces the same rows -- the demo can
be run four times in a week and the numbers never drift. Row counts are scaled
down from Northwind's real volumes (per the brief: ~4M shipment events/day,
~340 Airflow DAGs worth of freight data) to keep demo materialization fast
while keeping cardinalities and skew plausible for a mid-market 3PL.

Real Fivetran / carrier-API / Snowflake code paths never call anything in this
module -- these generators only run when a component's `demo_mode` is true.
"""

import hashlib

import numpy as np
import pandas as pd

from northwind_logistics.demo_data.heal_state import ANOMALY_CARRIER, ANOMALY_DATE, is_healed

CARRIERS = ("fedex", "ups", "regional_ltl_a", "regional_ltl_b")

# A couple of partitions get a volume bump to gesture at the Oct-Dec peak
# season Priya is nervous about, without building a full seasonal model.
PEAK_SKEW_DATES = ("2026-08-19", "2026-08-20")

_LANE_ORIGINS = ("ORD", "DFW", "ATL", "LAX", "JFK", "SEA", "MEM", "MSP")
_LANE_DESTS = ("PHX", "DEN", "CLT", "MIA", "BOS", "SLC", "PDX", "STL")
LANE_CODES = tuple(
    f"{origin}-{dest}"
    for origin, dest in zip(_LANE_ORIGINS, _LANE_DESTS, strict=True)
) + tuple(
    f"{origin}-{dest}"
    for origin, dest in zip(_LANE_DESTS, _LANE_ORIGINS, strict=True)
)

_CUSTOMER_PREFIXES = (
    "Meridian",
    "Atlas",
    "Summit",
    "Harbor",
    "Cascade",
    "Union",
    "Pioneer",
    "Vertex",
    "Granite",
    "Cobalt",
)
_CUSTOMER_SUFFIXES = (
    "Wholesale Foods",
    "Building Supply",
    "Apparel Group",
    "Electronics Distribution",
    "Furniture Co",
    "Industrial Supply",
    "Consumer Goods",
    "Manufacturing",
)
CUSTOMER_IDS = tuple(f"CUST-{i:04d}" for i in range(1, 41))
CUSTOMER_NAMES = tuple(
    f"{_CUSTOMER_PREFIXES[i % len(_CUSTOMER_PREFIXES)]} "
    f"{_CUSTOMER_SUFFIXES[i % len(_CUSTOMER_SUFFIXES)]}"
    for i in range(len(CUSTOMER_IDS))
)


def _seed_for(base_seed: int, partition_key: str) -> int:
    """Derive a stable per-partition seed from a base seed and partition key."""
    digest = hashlib.sha256(f"{base_seed}:{partition_key}".encode()).hexdigest()
    return int(digest[:8], 16)


def generate_carrier_rate_rows(rate_date: str, base_seed: int) -> pd.DataFrame:
    """Freight rate quotes for every carrier x lane on `rate_date`.

    Stands in for the FedEx / UPS / regional-LTL rate APIs. Northwind's real
    pain: two carriers land late ~15% of the time. That is modeled here as a
    planted, deterministic anomaly rather than a random one -- `regional_ltl_b`
    is entirely missing from `2026-08-21` until the partition is healed.
    """
    rng = np.random.default_rng(_seed_for(base_seed, rate_date))
    rows = []
    for carrier in CARRIERS:
        if carrier == ANOMALY_CARRIER and rate_date == ANOMALY_DATE and not is_healed(rate_date):
            continue
        base_rate = {"fedex": 4.10, "ups": 3.95, "regional_ltl_a": 2.60, "regional_ltl_b": 2.45}[
            carrier
        ]
        for lane_code in LANE_CODES:
            rate_amount = round(float(base_rate * rng.uniform(0.85, 1.25)), 2)
            fuel_surcharge_pct = round(float(rng.uniform(0.08, 0.22)), 4)
            transit_days = int(rng.integers(1, 6))
            rows.append(
                {
                    "carrier": carrier,
                    "rate_date": rate_date,
                    "lane_code": lane_code,
                    "rate_amount_usd": rate_amount,
                    "fuel_surcharge_pct": fuel_surcharge_pct,
                    "transit_days": transit_days,
                }
            )
    return pd.DataFrame(rows)


def generate_shipment_events_rows(event_date: str, base_seed: int, row_count: int = 2_500) -> pd.DataFrame:
    """Shipment scan events for `event_date`.

    Stands in for Northwind's event stream (~4M rows/day in production,
    scaled down here to keep `dg dev` materialization fast). A couple of
    partitions get a 3x volume bump to gesture at Oct-Dec peak season.
    """
    rng = np.random.default_rng(_seed_for(base_seed, event_date))
    effective_row_count = row_count * 3 if event_date in PEAK_SKEW_DATES else row_count
    carrier_idx = rng.integers(0, len(CARRIERS), size=effective_row_count)
    lane_idx = rng.integers(0, len(LANE_CODES), size=effective_row_count)
    customer_idx = rng.integers(0, len(CUSTOMER_IDS), size=effective_row_count)
    return pd.DataFrame(
        {
            "shipment_id": [f"SHP-{event_date.replace('-', '')}-{i:06d}" for i in range(effective_row_count)],
            "event_date": event_date,
            "carrier": np.array(CARRIERS)[carrier_idx],
            "lane_code": np.array(LANE_CODES)[lane_idx],
            "customer_id": np.array(CUSTOMER_IDS)[customer_idx],
            "weight_lbs": rng.uniform(15, 4_500, size=effective_row_count).round(1),
            "declared_value_usd": rng.uniform(50, 25_000, size=effective_row_count).round(2),
        }
    )


def generate_salesforce_accounts(base_seed: int) -> pd.DataFrame:
    """Salesforce `accounts` table -- Northwind's shipping customers."""
    rng = np.random.default_rng(_seed_for(base_seed, "salesforce_accounts"))
    return pd.DataFrame(
        {
            "customer_id": list(CUSTOMER_IDS),
            "account_name": list(CUSTOMER_NAMES),
            "account_owner": rng.choice(
                ["a.reyes", "j.chen", "m.oduya", "t.walsh"], size=len(CUSTOMER_IDS)
            ),
            "annual_shipment_volume_tier": rng.choice(
                ["enterprise", "mid_market", "growth"], size=len(CUSTOMER_IDS), p=[0.15, 0.45, 0.4]
            ),
        }
    )


def generate_zendesk_tickets(base_seed: int, row_count: int = 150) -> pd.DataFrame:
    """Zendesk support tickets -- includes the "missing invoice" complaints
    that are today Northwind's first signal a pipeline broke.
    """
    rng = np.random.default_rng(_seed_for(base_seed, "zendesk_tickets"))
    customer_idx = rng.integers(0, len(CUSTOMER_IDS), size=row_count)
    subjects = rng.choice(
        [
            "Missing invoice for last week's shipment",
            "Shipment tracking not updating",
            "Rate discrepancy on invoice",
            "Question about delivery window",
            "Billing address change request",
        ],
        size=row_count,
        p=[0.22, 0.28, 0.2, 0.2, 0.1],
    )
    return pd.DataFrame(
        {
            "ticket_id": [f"ZD-{i:05d}" for i in range(1, row_count + 1)],
            "customer_id": np.array(CUSTOMER_IDS)[customer_idx],
            "subject": subjects,
            "status": rng.choice(["open", "pending", "solved"], size=row_count, p=[0.15, 0.15, 0.7]),
        }
    )


def generate_netsuite_gl_entries(base_seed: int, row_count: int = 300) -> pd.DataFrame:
    """NetSuite general-ledger entries used to reconcile invoice billing."""
    rng = np.random.default_rng(_seed_for(base_seed, "netsuite_gl_entries"))
    customer_idx = rng.integers(0, len(CUSTOMER_IDS), size=row_count)
    return pd.DataFrame(
        {
            "gl_entry_id": [f"GL-{i:06d}" for i in range(1, row_count + 1)],
            "customer_id": np.array(CUSTOMER_IDS)[customer_idx],
            "account_code": rng.choice(["4000-FREIGHT-REV", "5100-CARRIER-COST"], size=row_count),
            "amount_usd": rng.uniform(-15_000, 40_000, size=row_count).round(2),
        }
    )
