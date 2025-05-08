import polars as pl
from polars.testing import assert_frame_equal

from loader import FundCsvDirectoryReportLoader
from test import specs_data_dir_path, test_data_dir_path


def load_spec_from_csv(test_name):
    path = specs_data_dir_path / f"{test_name}.csv"
    return pl.read_csv(path, try_parse_dates=True)


def test_e2e_can_load_csv_fund_report(request):
    config = FundCsvDirectoryReportLoader.Config(directory_path=str(test_data_dir_path))
    loader = FundCsvDirectoryReportLoader(config)
    results = loader.execute()
    expected = load_spec_from_csv(request.node.name)

    assert_frame_equal(results, expected)
