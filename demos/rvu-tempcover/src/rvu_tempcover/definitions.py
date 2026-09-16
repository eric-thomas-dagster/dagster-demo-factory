from pathlib import Path

from dagster import Definitions, definitions, in_process_executor, load_from_defs_folder

from rvu_tempcover.demo_data.bootstrap import ensure_raw_fixtures_loaded

ensure_raw_fixtures_loaded()


@definitions
def defs():
    # DuckDB is a single-writer file store; the default multiprocess executor
    # races workers to open `demo.duckdb` and blows up on the file lock. Force
    # in-process execution at the code-location level so `dg dev` materializations
    # stay serialized on one connection.
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions.merge(loaded, Definitions(executor=in_process_executor))
