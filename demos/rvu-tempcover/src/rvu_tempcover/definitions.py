from pathlib import Path

from dagster import definitions, load_from_defs_folder

from rvu_tempcover.demo_data.bootstrap import ensure_raw_fixtures_loaded

ensure_raw_fixtures_loaded()


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
