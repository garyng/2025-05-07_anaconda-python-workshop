import pathlib

import pandera.typing.polars as pat
import polars as pl

from loader import FundCsvDirectoryReportLoader, FundReportSchema

test_data_dir_path = pathlib.Path.cwd() / "data" / "external-funds"
specs_data_dir_path = pathlib.Path.cwd() / "test" / "specs"
test_db_path = pathlib.Path.cwd() / "db" / "testdb.sqlite"


def load_spec_from_csv(test_name):
    path = specs_data_dir_path / f"{test_name}.csv"
    return pl.read_csv(path, try_parse_dates=True)


def save_result_as_spec_csv(test_name, result: pl.DataFrame):
    path = specs_data_dir_path / f"{test_name}.csv"
    return result.write_csv(path)


def load_test_fund_report_data() -> pat.DataFrame[FundReportSchema]:
    config = FundCsvDirectoryReportLoader.Config(
        directory_path=str(test_data_dir_path),
        fund_name_mappings={},
    )
    loader = FundCsvDirectoryReportLoader(config)
    results = loader.execute()
    return results
