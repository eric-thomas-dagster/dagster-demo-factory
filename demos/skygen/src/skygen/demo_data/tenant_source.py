"""Simulated per-client SQL Server source databases.

Stands in for Skygen's ~170 per-client SQL Server databases (dental/vision
benefits SaaS and TPA data). The brief names the tenants only as "~170
clients", so tenants here are generic (`tenant_001` ...) and every column is
a plausible benefits-administration field, not a real client's data.

Deterministic: every `(table, tenant, date)` slice is generated from a seed
derived from those three values, so the same slice has the same rows on every
run, and the source system's own "how many rows exist" answer
(`expected_row_count`) is exactly what landing delivers. Mocks simulate the
source *system*; the assets that read from it stay idempotent.
"""

import hashlib
import random
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

TABLES = ("dental_claims", "vision_claims", "member_eligibility")

COLUMNS: dict[str, dict[str, str]] = {
    "dental_claims": {
        "claim_id": "VARCHAR",
        "tenant_id": "VARCHAR",
        "report_date": "DATE",
        "member_id": "VARCHAR",
        "procedure_code": "VARCHAR",
        "billed_amount": "DOUBLE",
        "paid_amount": "DOUBLE",
        "source_extracted_at": "TIMESTAMP",
    },
    "vision_claims": {
        "claim_id": "VARCHAR",
        "tenant_id": "VARCHAR",
        "report_date": "DATE",
        "member_id": "VARCHAR",
        "service_type": "VARCHAR",
        "billed_amount": "DOUBLE",
        "paid_amount": "DOUBLE",
        "source_extracted_at": "TIMESTAMP",
    },
    "member_eligibility": {
        "tenant_id": "VARCHAR",
        "report_date": "DATE",
        "member_id": "VARCHAR",
        "plan_type": "VARCHAR",
        "coverage_status": "VARCHAR",
        "source_extracted_at": "TIMESTAMP",
    },
}

_DENTAL_PROCEDURES = ("D0120", "D0150", "D1110", "D2391", "D2740", "D4341", "D7140")
_VISION_SERVICES = ("exam", "frames", "lenses", "contacts")
_PLAN_TYPES = ("dental", "vision", "dental_vision")


def _rng(table: str, tenant: str, day: str) -> random.Random:
    digest = hashlib.sha256(f"skygen|{table}|{tenant}|{day}".encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _tenant_scale(tenant: str) -> int:
    """Members enrolled for a tenant: fixed per tenant, 120-360."""
    digest = hashlib.sha256(f"skygen|scale|{tenant}".encode()).digest()
    return 120 + digest[0] % 241


def _member_id(tenant: str, n: int) -> str:
    return f"{tenant}-m{n:05d}"


def extracted_at(day: str) -> datetime:
    """The nightly extract finishes at 02:00 UTC the morning after `day`."""
    return datetime.combine(date.fromisoformat(day) + timedelta(days=1), time(2, 0), tzinfo=timezone.utc).replace(
        tzinfo=None
    )


def rows_for(table: str, tenant: str, day: str) -> list[tuple[Any, ...]]:
    """The rows `table` holds for `(tenant, day)` in the tenant's SQL Server DB."""
    rng = _rng(table, tenant, day)
    members = _tenant_scale(tenant)
    stamp = extracted_at(day) + timedelta(minutes=rng.randint(0, 40))
    report_date = date.fromisoformat(day)

    if table == "member_eligibility":
        return [
            (
                tenant,
                report_date,
                _member_id(tenant, n),
                _PLAN_TYPES[n % len(_PLAN_TYPES)],
                "active" if rng.random() > 0.04 else "termed",
                stamp,
            )
            for n in range(1, members + 1)
        ]

    claim_count = max(15, members // 4 + rng.randint(-8, 8))
    rows: list[tuple[Any, ...]] = []
    for n in range(claim_count):
        billed = round(rng.uniform(40, 1200) if table == "dental_claims" else rng.uniform(25, 450), 2)
        paid = round(billed * rng.uniform(0.55, 0.9), 2)
        detail = (
            rng.choice(_DENTAL_PROCEDURES) if table == "dental_claims" else rng.choice(_VISION_SERVICES)
        )
        rows.append(
            (
                f"{tenant}-{table[:1]}{day.replace('-', '')}-{n:05d}",
                tenant,
                report_date,
                _member_id(tenant, rng.randint(1, members)),
                detail,
                billed,
                paid,
                stamp,
            )
        )
    return rows


def expected_row_count(table: str, tenant: str, day: str) -> int:
    """What the source system itself says it holds -- the denominator for the
    Fivetran-landed completeness check."""
    return len(rows_for(table, tenant, day))
