.PHONY: reset-demo

# Resets the mock source system, never Dagster. Deletes the local DuckDB
# warehouse and the carrier-feed arrival state so the planted anomaly
# (regional_ltl_b / 2026-08-21) is unhealed again on the next materialize.
reset-demo:
	rm -f src/northwind_logistics/demo_data/demo.duckdb
	rm -f src/northwind_logistics/demo_data/_source_state.json
	@echo "Demo reset. Next 'dg launch --assets ...' regenerates the warehouse from scratch."
