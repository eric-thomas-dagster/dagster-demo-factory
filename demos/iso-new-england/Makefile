.PHONY: reset-demo simulate-event

# Resets the mock source system, never Dagster. Deletes the local DuckDB
# warehouse and the advisory-feed arrival state, so the next `dg dev` starts
# from a fresh, fully-seeded historical window (see demo_data/feed_state.py).
reset-demo:
	rm -f src/iso_new_england/demo_data/demo.duckdb
	rm -f src/iso_new_england/demo_data/_advisory_feed_state.json
	@echo "Demo reset. Next materialize regenerates the warehouse and re-seeds the mock feed."

# Live-demo helper: marks "today" (or a given DATE=YYYY-MM-DD) as arrived in
# the mock advisory feed, for external_feed_arrival_sensor to pick up.
simulate-event:
	python -m iso_new_england.demo_data.simulate_new_advisory $(DATE)
