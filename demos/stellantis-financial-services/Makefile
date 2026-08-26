.PHONY: reset-demo simulate-correction

# Resets the mock source system, never Dagster. Deletes the local DuckDB
# warehouse and the floorplan-feed correction state, so the next `dg dev`
# starts from a fresh demo with the planted anomaly back in place.
reset-demo:
	rm -f src/stellantis_financial_services/demo_data/demo.duckdb
	rm -f src/stellantis_financial_services/demo_data/_floorplan_source_state.json
	@echo "Demo reset. Next materialize regenerates the warehouse and re-plants the anomaly."

# Live-demo helper: simulates the flagged dealer resending a corrected
# floorplan file. Run this between showing the blocking check fail and
# rematerializing raw_dealer_floorplan_feed for the 2026-08-22 |
# midwest_dealers partition.
simulate-correction:
	python -c "from stellantis_financial_services.demo_data.fabric_source_state import mark_corrected; mark_corrected(); print('Corrected file received -- rematerialize raw_dealer_floorplan_feed for 2026-08-22 | midwest_dealers.')"
