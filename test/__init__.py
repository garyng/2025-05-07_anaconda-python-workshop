import pathlib

import polars as pl

test_data_dir_path = pathlib.Path.cwd() / "data" / "external-funds"
specs_data_dir_path = pathlib.Path.cwd() / "test" / "specs"


def load_spec_from_csv(test_name):
    path = specs_data_dir_path / f"{test_name}.csv"
    return pl.read_csv(path, try_parse_dates=True)


def save_result_as_spec_csv(test_name, result: pl.DataFrame):
    path = specs_data_dir_path / f"{test_name}.csv"
    return result.write_csv(path)
