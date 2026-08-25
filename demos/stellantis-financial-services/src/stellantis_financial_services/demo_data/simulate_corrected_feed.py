"""Simulates the dealer resending a corrected floorplan file for the planted-anomaly date.

Run with: `python -m stellantis_financial_services.demo_data.simulate_corrected_feed`

This is an operation on the mock vendor source, never on Dagster -- per
CLAUDE.md's idempotency rule, there is no heal asset or reset job.
Rematerializing `raw_dealer_floorplan_feed` for {ANOMALY_DATE} after running
this picks up the corrected file, exactly as it would against the real
vendor feed.
"""

from stellantis_financial_services.demo_data.vendor_state import ANOMALY_DATE, ANOMALY_FEED, mark_corrected


def main() -> None:
    mark_corrected()
    print(
        f"Vendor source updated: {ANOMALY_FEED} for {ANOMALY_DATE} is now the corrected file. "
        f"Rematerialize raw_dealer_floorplan_feed for partition {ANOMALY_DATE} to pick it up."
    )


if __name__ == "__main__":
    main()
