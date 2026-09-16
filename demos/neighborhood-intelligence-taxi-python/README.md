# neighborhood-intelligence-taxi-python

The pure-Python counterfactual to the sibling
[`demos/neighborhood-intelligence-taxi/`](../neighborhood-intelligence-taxi/)
project. Same asset graph -- 2 bronze external assets, 2 silver, 4 gold, 2
report, 3 blocking checks on `fct_trips`, 1 monthly schedule -- but every
asset is a hand-written `@asset` function against DuckDB rather than a
`dagster-dbt` model + a Dagster Community Components (DCC) YAML wiring. It
exists so a prospect can see the side-by-side: `dbt + DCC` vs. raw Dagster
Python code volume for identical behavior. See the parent project's
[`WALKTHROUGH_PYTHON.md`](../neighborhood-intelligence-taxi/WALKTHROUGH_PYTHON.md)
for the annotated walkthrough. This project deliberately does **not** follow
the parent's [`PATTERNS.md`](../neighborhood-intelligence-taxi/PATTERNS.md)
(dbt-first, registry-only, YAML-first) -- that is the point. It exists to
show what you have to write when you don't.

## Run

```bash
uv sync
uv run python validate_e2e.py    # green out of the gate
uv run dg dev                    # UI at http://localhost:3000
```

Zero-setup: the DuckDB warehouse and its fixture data are seeded on first
import from `demo_data/bootstrap.py`. No env vars required.
